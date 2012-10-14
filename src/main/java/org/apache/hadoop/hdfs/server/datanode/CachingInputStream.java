/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

public class CachingInputStream extends InputStream {

	private final ExtendedBlock block;

	private final BlockCache blockCache;

	private final InputStream inputStream;

	private final ArrayList<byte[]> cachedBuffers = new ArrayList<byte[]>();

	private boolean addToCache;

	private byte[] currentBuffer = null;

	private int currentBufferOffset = 0;

	private boolean readMinusOne = false;

	CachingInputStream(final ExtendedBlock block, final BlockCache blockCache, final InputStream inputStream,
			final boolean addToCache) {
		this.block = block;
		this.blockCache = blockCache;
		this.inputStream = inputStream;
		this.addToCache = addToCache;
	}

	private final void clearCachedBuffers() {

		final Iterator<byte[]> it = this.cachedBuffers.iterator();
		while (it.hasNext()) {
			this.blockCache.returnBuffer(it.next());
		}
		this.cachedBuffers.clear();

		if (this.currentBuffer != null) {
			this.blockCache.returnBuffer(this.currentBuffer);
			this.currentBuffer = null;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int available() {
		throw new UnsupportedOperationException("Available");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		// Make sure there is no more data in the stream that we need to cache
		if (this.currentBuffer != null) {
			this.cachedBuffers.add(this.currentBuffer);
			this.currentBuffer = null;
		}

		if (this.addToCache && !this.readMinusOne) {

			if (this.inputStream.read() != -1) {
				// The is more data to read from this block that is not in the cache, discard cache
				clearCachedBuffers();
			}
		}

		if (!this.cachedBuffers.isEmpty()) {
			final CachedBlock cachedBlock = new CachedBlock(this.cachedBuffers,
				((long) (this.cachedBuffers.size() - 1) * (long) this.blockCache.getBufferSize())
					+ (long) this.currentBufferOffset);
			this.blockCache.addCachedBlock(this.block, cachedBlock);
		}

		this.inputStream.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void mark(final int readlimit) {
		throw new UnsupportedOperationException("mark");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean markSupported() {
		throw new UnsupportedOperationException("markSupported");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read() throws IOException {
		throw new UnsupportedOperationException("read");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(byte[] b, int off, int len) throws IOException {

		if (!this.addToCache) {
			return this.inputStream.read(b, off, len);
		}

		if (this.currentBuffer == null) {
			this.currentBuffer = this.blockCache.requestBuffer();
			if (this.currentBuffer == null) {
				this.addToCache = false;
				clearCachedBuffers();
				return 0;
			}

			this.currentBufferOffset = 0;
		}

		final int dataToRead = Math.min(len, this.currentBuffer.length - this.currentBufferOffset);
		final int read = this.inputStream.read(b, off, dataToRead);
		if (read == -1) {
			this.cachedBuffers.add(this.currentBuffer);
			this.currentBuffer = null;
			this.readMinusOne = true;
		} else {
			System.arraycopy(b, off, this.currentBuffer, this.currentBufferOffset, read);
			this.currentBufferOffset += read;
			if (this.currentBufferOffset == this.currentBuffer.length) {
				this.cachedBuffers.add(this.currentBuffer);
				this.currentBuffer = null;
			}
		}

		return read;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reset() {
		throw new UnsupportedOperationException("reset");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long skip(long n) throws IOException {

		final long retVal = this.inputStream.skip(n);

		// When data is skipped, we cannot cache it
		if (this.addToCache && retVal > 0L) {
			clearCachedBuffers();
			this.addToCache = false;
		}

		return retVal;
	}
}
