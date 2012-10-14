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
import java.util.List;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

public class CachedInputStream extends InputStream {

	private final ExtendedBlock block;

	private final BlockCache blockCache;

	private final List<byte[]> cachedBuffers;

	private final long length;

	private byte[] currentBuffer = null;

	private int currentBufferLength = 0;

	private int indexToNextBuffer = 0;

	private int readInBuffer = 0;

	private long readInTotal = 0;

	CachedInputStream(final ExtendedBlock block, final BlockCache blockCache, final CachedBlock cachedBlock) {

		this.block = block;
		this.blockCache = blockCache;
		this.cachedBuffers = cachedBlock.getCachedBuffers();
		this.length = cachedBlock.getLength();
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

		this.currentBuffer = null;
		this.currentBufferLength = 0;
		this.indexToNextBuffer = 0;
		this.readInBuffer = 0;
		this.readInTotal = 0;

		this.blockCache.unlock(this.block);
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
	public int read(final byte[] b, final int off, final int len) throws IOException {

		if (allBuffersConsumed()) {
			return -1;
		}

		final int dataToRead = Math.min(len, this.currentBufferLength - this.readInBuffer);

		System.arraycopy(this.currentBuffer, this.readInBuffer, b, off, dataToRead);
		this.readInBuffer += dataToRead;
		this.readInTotal += dataToRead;

		if (this.readInBuffer == this.currentBufferLength) {
			this.currentBuffer = null;
		}

		return dataToRead;
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
	public long skip(final long n) throws IOException {

		final long remainingData = this.length - this.readInTotal;
		if (n >= remainingData) {
			this.indexToNextBuffer = this.cachedBuffers.size();
			this.currentBuffer = null;
			return remainingData;
		}

		final long positionToSkipTo = this.readInTotal + (int) n;
		while (this.readInTotal < positionToSkipTo) {

			if (allBuffersConsumed()) {
				throw new IllegalStateException("Unexpected end of buffers");
			}

			final int skip = this.currentBufferLength - this.readInBuffer;

			this.readInBuffer += skip;
			this.readInTotal += skip;

			if (this.readInBuffer == this.currentBufferLength) {
				this.currentBuffer = null;
			}
		}

		return n;
	}

	/**
	 * Checks if all buffers have been consumed and also updates the references to the current buffers if necessary.
	 * 
	 * @return <code>true</code> if all buffers have been consumed, <code>false</code> otherwise
	 */
	private boolean allBuffersConsumed() {

		// Check if we have to switch to the next buffer
		if (this.currentBuffer == null) {
			final int size = this.cachedBuffers.size();
			if (this.indexToNextBuffer == size) {
				return true;
			}

			this.currentBuffer = this.cachedBuffers.get(this.indexToNextBuffer++);
			this.currentBufferLength = (this.indexToNextBuffer == size) ? (int) (this.length - this.readInTotal)
				: this.currentBuffer.length;
			this.readInBuffer = 0;
		}

		return false;
	}
}
