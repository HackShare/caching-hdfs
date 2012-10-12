package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;

public class CachingInputStream extends InputStream {

	private final BlockCache blockCache;

	private final InputStream inputStream;

	private final ArrayDeque<byte[]> cachedBuffers = new ArrayDeque<byte[]>();

	private boolean addToCache;

	private byte[] currentBuffer = null;

	private int currentBufferOffset = 0;

	private boolean readMinusOne = false;

	CachingInputStream(final BlockCache blockCache, final InputStream inputStream, final boolean addToCache) {
		this.blockCache = blockCache;
		this.inputStream = inputStream;
		this.addToCache = addToCache;
	}

	private final void clearCachedBuffers() {

		while (!this.cachedBuffers.isEmpty()) {
			this.blockCache.returnBuffer(this.cachedBuffers.poll());
		}
	}

	@Override
	public int available() {
		System.out.println("Available");

		return 0;
	}

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
			System.out.println("Cached " + this.cachedBuffers.size() + " blockes");
			System.out.println(this.currentBufferOffset);
		}

		this.inputStream.close();
	}

	@Override
	public void mark(int readlimit) {
		System.out.println("mark");
	}

	@Override
	public boolean markSupported() {

		System.out.println("markSupported");

		return false;
	}

	@Override
	public int read() throws IOException {

		System.out.println("read 1 ");

		return 0;
	}

	@Override
	public int read(final byte[] b) throws IOException {

		return read(b, 0, b.length);
	}

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

	@Override
	public void reset() {

		System.out.println("reset");
	}

	@Override
	public long skip(long n) {

		System.out.println("skip");

		return 0L;
	}
}
