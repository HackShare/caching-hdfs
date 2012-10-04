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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.DataChecksum;

/**
 * Reads a block from the disk and sends it to a recipient.
 * Data sent from the BlockeSender in the following format: <br>
 * <b>Data format:</b>
 * 
 * <pre>
 *    +--------------------------------------------------+
 *    | ChecksumHeader | Sequence of data PACKETS...     |
 *    +--------------------------------------------------+
 * </pre>
 * 
 * <b>ChecksumHeader format:</b>
 * 
 * <pre>
 *    +--------------------------------------------------+
 *    | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
 *    +--------------------------------------------------+
 * </pre>
 * 
 * An empty packet is sent to mark the end of block and read completion.
 * PACKET Contains a packet header, checksum and data. Amount of data
 * carried is set by BUFFER_SIZE.
 * 
 * <pre>
 *    +-----------------------------------------------------+
 *    | 4 byte packet length (excluding packet header)      |
 *    +-----------------------------------------------------+
 *    | 8 byte offset in the block | 8 byte sequence number |
 *    +-----------------------------------------------------+
 *    | 1 byte isLastPacketInBlock                          |
 *    +-----------------------------------------------------+
 *    | 4 byte Length of actual data                        |
 *    +-----------------------------------------------------+
 *    | x byte checksum data. x is defined below            |
 *    +-----------------------------------------------------+
 *    | actual data ......                                  |
 *    +-----------------------------------------------------+
 *    
 *    Data is made of Chunks. Each chunk is of length <= BYTES_PER_CHECKSUM.
 *    A checksum is calculated for each chunk.
 *    
 *    x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
 *        CHECKSUM_SIZE
 *        
 *    CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32)
 * </pre>
 * 
 * The client reads data until it receives a packet with
 * "LastPacketInBlock" set to true or with a zero length. If there is
 * no checksum error, it replies to DataNode with OP_STATUS_CHECKSUM_OK:
 * 
 * <pre>
 *    +------------------------------+
 *    | 2 byte OP_STATUS_CHECKSUM_OK |
 *    +------------------------------+
 * </pre>
 */
class CachingBlockSender implements java.io.Closeable {

	static final Log LOG = CachingDataNode.LOG;

	static final Log ClientTraceLog = DataNode.ClientTraceLog;

	private static final boolean is32Bit = System.getProperty("sun.arch.data.model").equals("32");

	/**
	 * Minimum buffer used while sending data to clients. Used only if
	 * transferTo() is enabled. 64KB is not that large. It could be larger, but
	 * not sure if there will be much more improvement.
	 */
	private static final int MIN_BUFFER_WITH_TRANSFERTO = 64 * 1024;

	private static final int TRANSFERTO_BUFFER_SIZE = Math.max(HdfsConstants.IO_FILE_BUFFER_SIZE,
		MIN_BUFFER_WITH_TRANSFERTO);

	private final BlockCache blockCache;

	/**
	 * The block to read from
	 */
	private final ExtendedBlock block;

	/**
	 * Stream to read block data from
	 */
	private InputStream blockIn;

	/**
	 * Updated while using transferTo()
	 */
	private long blockInPosition = -1L;

	/**
	 * Stream to read checksum
	 */
	private DataInputStream checksumIn;

	/**
	 * Checksum utility
	 */
	private final DataChecksum checksum;

	/**
	 * Initial position to read
	 */
	private long initialOffset;

	/**
	 * Current position of read
	 */
	private long offset;

	/**
	 * Position of last byte to read from block file
	 */
	private final long endOffset;

	/**
	 * Number of bytes in chunk used for computing checksum
	 */
	private final int chunkSize;

	/**
	 * Number bytes of checksum computed for a chunk
	 */
	private final int checksumSize;

	/**
	 * If true, failure to read checksum is ignored
	 */
	private final boolean corruptChecksumOk;

	/**
	 * Sequence number of packet being sent
	 */
	private long seqno;

	/**
	 * Set to true if transferTo is allowed for sending data to the client
	 */
	private final boolean transferToAllowed;

	/**
	 * Set to true once entire requested byte range has been sent to the client
	 */
	private boolean sentEntireByteRange;

	/**
	 * When true, verify checksum while reading from checksum file
	 */
	private final boolean verifyChecksum;

	/**
	 * Format used to print client trace log messages
	 */
	private final String clientTraceFmt;

	private volatile ChunkChecksum lastChunkChecksum = null;

	/**
	 * The file descriptor of the block being sent
	 */
	private FileDescriptor blockInFd;

	/**
	 * Cache-management related fields
	 */
	private final long readaheadLength;

	private boolean shouldDropCacheBehindRead;

	private ReadaheadRequest curReadahead;

	private long lastCacheDropOffset;

	private static final long CACHE_DROP_INTERVAL_BYTES = 1024 * 1024; // 1MB

	/**
	 * Minimum length of read below which management of the OS
	 * buffer cache is disabled.
	 */
	private static final long LONG_READ_THRESHOLD_BYTES = 256 * 1024;

	private static ReadaheadPool readaheadPool = ReadaheadPool.getInstance();

	/**
	 * Constructor
	 * 
	 * @param block
	 *        Block that is being read
	 * @param startOffset
	 *        starting offset to read from
	 * @param length
	 *        length of data to read
	 * @param corruptChecksumOk
	 * @param verifyChecksum
	 *        verify checksum while reading the data
	 * @param datanode
	 *        datanode from which the block is being read
	 * @param clientTraceFmt
	 *        format string used to print client trace logs
	 * @throws IOException
	 */
	CachingBlockSender(final BlockCache blockCache, final ExtendedBlock block, final long startOffset, long length,
			final boolean corruptChecksumOk, final boolean verifyChecksum, final DataNode datanode,
			final String clientTraceFmt) throws IOException {

		this.blockCache = blockCache;
		this.block = block;
		this.corruptChecksumOk = corruptChecksumOk;
		this.verifyChecksum = verifyChecksum;
		this.clientTraceFmt = clientTraceFmt;
		this.readaheadLength = datanode.getDnConf().readaheadLength;
		this.shouldDropCacheBehindRead = datanode.getDnConf().dropCacheBehindReads;

		try {

			final Replica replica;
			final long replicaVisibleLength;
			synchronized (datanode.data) {
				replica = getReplica(block, datanode);
				replicaVisibleLength = replica.getVisibleLength();
			}
			// if there is a write in progress
			ChunkChecksum chunkChecksum = null;
			if (replica instanceof ReplicaBeingWritten) {
				final ReplicaBeingWritten rbw = (ReplicaBeingWritten) replica;
				waitForMinLength(rbw, startOffset + length);
				chunkChecksum = rbw.getLastChecksumAndDataLen();
			}

			if (replica.getGenerationStamp() < block.getGenerationStamp()) {
				throw new IOException("Replica gen stamp < block genstamp, block=" + block + ", replica=" + replica);
			}
			if (replicaVisibleLength < 0L) {
				throw new IOException("Replica is not readable, block=" + block + ", replica=" + replica);
			}
			if (DataNode.LOG.isDebugEnabled()) {
				DataNode.LOG.debug("block=" + block + ", replica=" + replica);
			}

			// transferToFully() fails on 32 bit platforms for block sizes >= 2GB, use normal transfer in those cases
			this.transferToAllowed = datanode.getDnConf().transferToAllowed
				&& (!is32Bit || length <= Integer.MAX_VALUE);

			/*
			 * (corruptChecksumOK, meta_file_exist): operation
			 * True, True: will verify checksum
			 * True, False: No verify, e.g., need to read data from a corrupted file
			 * False, True: will verify checksum
			 * False, False: throws IOException file not found
			 */
			DataChecksum csum;
			final InputStream metaIn = datanode.data.getMetaDataInputStream(block);
			if (!corruptChecksumOk || metaIn != null) {
				if (metaIn == null) {
					// need checksum but meta-data not found
					throw new FileNotFoundException("Meta-data not found for " + block);
				}

				this.checksumIn = new DataInputStream(
					new BufferedInputStream(metaIn, HdfsConstants.IO_FILE_BUFFER_SIZE));

				// read and handle the common header here. For now just a version
				final BlockMetadataHeader header = BlockMetadataHeader.readHeader(this.checksumIn);
				short version = header.getVersion();
				if (version != BlockMetadataHeader.VERSION) {
					LOG.warn("Wrong version (" + version + ") for metadata file for " + block + " ignoring ...");
				}
				csum = header.getChecksum();
			} else {
				LOG.warn("Could not find metadata file for " + block);
				// This only decides the buffer size. Use BUFFER_SIZE?
				csum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_NULL, 16 * 1024);
			}

			/*
			 * If chunkSize is very large, then the metadata file is mostly
			 * corrupted. For now just truncate bytesPerchecksum to blockLength.
			 */
			int size = csum.getBytesPerChecksum();
			if (size > 10 * 1024 * 1024 && size > replicaVisibleLength) {
				csum = DataChecksum.newDataChecksum(csum.getChecksumType(),
					Math.max((int) replicaVisibleLength, 10 * 1024 * 1024));
				size = csum.getBytesPerChecksum();
			}
			this.chunkSize = size;
			this.checksum = csum;
			this.checksumSize = checksum.getChecksumSize();
			length = length < 0 ? replicaVisibleLength : length;

			// end is either last byte on disk or the length for which we have a checksum
			long end = chunkChecksum != null ? chunkChecksum.getDataLength() : replica.getBytesOnDisk();
			if (startOffset < 0 || startOffset > end || (length + startOffset) > end) {
				final String msg = " Offset " + startOffset + " and length " + length + " don't match block " + block
					+ " ( blockLen " + end + " )";
				LOG.warn(datanode.getDNRegistrationForBP(block.getBlockPoolId()) + ":sendBlock() : " + msg);
				throw new IOException(msg);
			}

			// Ensure read offset is position at the beginning of chunk
			this.offset = startOffset - (startOffset % this.chunkSize);
			if (length >= 0) {
				// Ensure endOffset points to end of chunk.
				long tmpLen = startOffset + length;
				if (tmpLen % this.chunkSize != 0) {
					tmpLen += (this.chunkSize - tmpLen % this.chunkSize);
				}
				if (tmpLen < end) {
					// will use on-disk checksum here since the end is a stable chunk
					end = tmpLen;
				} else if (chunkChecksum != null) {
					// last chunk is changing. flag that we need to use in-memory checksum
					this.lastChunkChecksum = chunkChecksum;
				}
			}
			this.endOffset = end;

			// seek to the right offsets
			if (this.offset > 0) {
				long checksumSkip = (this.offset / this.chunkSize) * this.checksumSize;
				// note blockInStream is seeked when created below
				if (checksumSkip > 0) {
					// Should we use seek() for checksum file as well?
					IOUtils.skipFully(this.checksumIn, checksumSkip);
				}
			}
			this.seqno = 0;

			if (DataNode.LOG.isDebugEnabled()) {
				DataNode.LOG.debug("replica=" + replica);
			}
			this.blockIn = datanode.data.getBlockInputStream(block, this.offset); // seek to offset
			if (this.blockIn instanceof FileInputStream) {
				this.blockInFd = ((FileInputStream) blockIn).getFD();
			} else {
				this.blockInFd = null;
			}
		} catch (IOException ioe) {
			IOUtils.closeStream(this);
			IOUtils.closeStream(this.blockIn);
			throw ioe;
		}
	}

	/**
	 * close opened files.
	 */
	public void close() throws IOException {

		if (this.blockInFd != null && this.shouldDropCacheBehindRead && isLongRead()) {
			// drop the last few MB of the file from cache
			try {
				NativeIO.posixFadviseIfPossible(this.blockInFd, this.lastCacheDropOffset, this.offset
					- this.lastCacheDropOffset, NativeIO.POSIX_FADV_DONTNEED);
			} catch (Exception e) {
				LOG.warn("Unable to drop cache on file close", e);
			}
		}
		if (this.curReadahead != null) {
			this.curReadahead.cancel();
		}

		IOException ioe = null;
		if (this.checksumIn != null) {
			try {
				this.checksumIn.close(); // close checksum file
			} catch (IOException e) {
				ioe = e;
			}
			this.checksumIn = null;
		}
		if (this.blockIn != null) {
			try {
				this.blockIn.close(); // close data file
			} catch (IOException e) {
				ioe = e;
			}
			this.blockIn = null;
			this.blockInFd = null;
		}
		// throw IOException if there is any
		if (ioe != null) {
			throw ioe;
		}
	}

	private static Replica getReplica(final ExtendedBlock block, final DataNode datanode)
			throws ReplicaNotFoundException {

		final Replica replica = datanode.data.getReplica(block.getBlockPoolId(), block.getBlockId());
		if (replica == null) {
			throw new ReplicaNotFoundException(block);
		}
		return replica;
	}

	/**
	 * Wait for rbw replica to reach the length
	 * 
	 * @param rbw
	 *        replica that is being written to
	 * @param len
	 *        minimum length to reach
	 * @throws IOException
	 *         on failing to reach the len in given wait time
	 */
	private static void waitForMinLength(final ReplicaBeingWritten rbw, final long len) throws IOException {

		// Wait for 3 seconds for rbw replica to reach the minimum length
		for (int i = 0; i < 30 && rbw.getBytesOnDisk() < len; i++) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException ie) {
				throw new IOException(ie);
			}
		}
		final long bytesOnDisk = rbw.getBytesOnDisk();
		if (bytesOnDisk < len) {
			throw new IOException(String.format("Need %d bytes, but only %d bytes available", len, bytesOnDisk));
		}
	}

	/**
	 * Converts an IOExcpetion (not subclasses) to SocketException.
	 * This is typically done to indicate to upper layers that the error
	 * was a socket error rather than often more serious exceptions like
	 * disk errors.
	 */
	private static IOException ioeToSocketException(IOException ioe) {

		if (ioe.getClass().equals(IOException.class)) {
			// "se" could be a new class in stead of SocketException.
			final IOException se = new SocketException("Original Exception : " + ioe);
			se.initCause(ioe);
			/*
			 * Change the stacktrace so that original trace is not truncated
			 * when printed.
			 */
			se.setStackTrace(ioe.getStackTrace());
			return se;
		}
		// otherwise just return the same exception.
		return ioe;
	}

	/**
	 * @param datalen
	 *        Length of data
	 * @return number of chunks for data of given size
	 */
	private int numberOfChunks(final long datalen) {
		return (int) ((datalen + chunkSize - 1) / chunkSize);
	}

	/**
	 * Sends a packet with up to maxChunks chunks of data.
	 * 
	 * @param pkt
	 *        buffer used for writing packet data
	 * @param maxChunks
	 *        maximum number of chunks to send
	 * @param out
	 *        stream to send data to
	 * @param transferTo
	 *        use transferTo to send data
	 * @param throttler
	 *        used for throttling data transfer bandwidth
	 */
	private int sendPacket(final ByteBuffer pkt, final int maxChunks, final OutputStream out, final boolean transferTo,
			final DataTransferThrottler throttler) throws IOException {

		final int dataLen = (int) Math.min(this.endOffset - this.offset, (this.chunkSize * (long) maxChunks));

		final int numChunks = numberOfChunks(dataLen); // Number of chunks be sent in the packet
		final int checksumDataLen = numChunks * checksumSize;
		final int packetLen = dataLen + checksumDataLen + 4;
		boolean lastDataPacket = offset + dataLen == endOffset && dataLen > 0;

		writePacketHeader(pkt, dataLen, packetLen);

		final int checksumOff = pkt.position();
		final byte[] buf = pkt.array();

		if (this.checksumSize > 0 && this.checksumIn != null) {
			readChecksum(buf, checksumOff, checksumDataLen);

			// write in progress that we need to use to get last checksum
			if (lastDataPacket && this.lastChunkChecksum != null) {
				final int start = checksumOff + checksumDataLen - checksumSize;
				final byte[] updatedChecksum = this.lastChunkChecksum.getChecksum();

				if (updatedChecksum != null) {
					System.arraycopy(updatedChecksum, 0, buf, start, checksumSize);
				}
			}
		}

		final int dataOff = checksumOff + checksumDataLen;
		if (!transferTo) { // normal transfer
			IOUtils.readFully(this.blockIn, buf, dataOff, dataLen);

			if (this.verifyChecksum) {
				verifyChecksum(buf, dataOff, dataLen, numChunks, checksumOff);
			}
		}

		try {
			if (transferTo) {
				final SocketOutputStream sockOut = (SocketOutputStream) out;
				sockOut.write(buf, 0, dataOff); // First write checksum

				// no need to flush. since we know out is not a buffered stream.
				sockOut.transferToFully(((FileInputStream) this.blockIn).getChannel(), this.blockInPosition, dataLen);
				this.blockInPosition += dataLen;
			} else {
				// normal transfer
				out.write(buf, 0, dataOff + dataLen);
			}
		} catch (IOException e) {
			/*
			 * Exception while writing to the client. Connection closure from
			 * the other end is mostly the case and we do not care much about
			 * it. But other things can go wrong, especially in transferTo(),
			 * which we do not want to ignore.
			 * The message parsing below should not be considered as a good
			 * coding example. NEVER do it to drive a program logic. NEVER.
			 * It was done here because the NIO throws an IOException for EPIPE.
			 */
			final String ioem = e.getMessage();
			if (!ioem.startsWith("Broken pipe") && !ioem.startsWith("Connection reset")) {
				LOG.error("BlockSender.sendChunks() exception: ", e);
			}
			throw ioeToSocketException(e);
		}

		if (throttler != null) { // rebalancing so throttle
			throttler.throttle(packetLen);
		}

		return dataLen;
	}

	/**
	 * Read checksum into given buffer
	 * 
	 * @param buf
	 *        buffer to read the checksum into
	 * @param checksumOffset
	 *        offset at which to write the checksum into buf
	 * @param checksumLen
	 *        length of checksum to write
	 * @throws IOException
	 *         on error
	 */
	private void readChecksum(byte[] buf, final int checksumOffset, final int checksumLen) throws IOException {
		if (this.checksumSize <= 0 && this.checksumIn == null) {
			return;
		}
		try {
			this.checksumIn.readFully(buf, checksumOffset, checksumLen);
		} catch (IOException e) {
			LOG.warn(" Could not read or failed to veirfy checksum for data"
				+ " at offset " + offset + " for block " + block, e);
			IOUtils.closeStream(this.checksumIn);
			this.checksumIn = null;
			if (this.corruptChecksumOk) {
				if (checksumOffset < checksumLen) {
					// Just fill the array with zeros.
					Arrays.fill(buf, checksumOffset, checksumLen, (byte) 0);
				}
			} else {
				throw e;
			}
		}
	}

	/**
	 * Compute checksum for chunks and verify the checksum that is read from
	 * the metadata file is correct.
	 * 
	 * @param buf
	 *        buffer that has checksum and data
	 * @param dataOffset
	 *        position where data is written in the buf
	 * @param datalen
	 *        length of data
	 * @param numChunks
	 *        number of chunks corresponding to data
	 * @param checksumOffset
	 *        offset where checksum is written in the buf
	 * @throws ChecksumException
	 *         on failed checksum verification
	 */
	public void verifyChecksum(final byte[] buf, final int dataOffset, final int datalen, final int numChunks,
			final int checksumOffset) throws ChecksumException {

		int dOff = dataOffset;
		int cOff = checksumOffset;
		int dLeft = datalen;

		for (int i = 0; i < numChunks; i++) {
			this.checksum.reset();
			int dLen = Math.min(dLeft, this.chunkSize);
			this.checksum.update(buf, dOff, dLen);
			if (!checksum.compare(buf, cOff)) {
				long failedPos = this.offset + datalen - dLeft;
				throw new ChecksumException("Checksum failed at " + failedPos,
					failedPos);
			}
			dLeft -= dLen;
			dOff += dLen;
			cOff += this.checksumSize;
		}
	}

	/**
	 * sendBlock() is used to read block and its metadata and stream the data to
	 * either a client or to another datanode.
	 * 
	 * @param out
	 *        stream to which the block is written to
	 * @param baseStream
	 *        optional. if non-null, <code>out</code> is assumed to
	 *        be a wrapper over this stream. This enables optimizations for
	 *        sending the data, e.g. {@link SocketOutputStream#transferToFully(FileChannel, long, int)}.
	 * @param throttler
	 *        for sending data.
	 * @return total bytes read, including checksum data.
	 */
	long sendBlock(final DataOutputStream out, final OutputStream baseStream, final DataTransferThrottler throttler)
			throws IOException {

		if (out == null) {
			throw new IOException("out stream is null");
		}

		this.initialOffset = this.offset;
		long totalRead = 0;
		OutputStream streamForSendChunks = out;

		this.lastCacheDropOffset = this.initialOffset;

		if (isLongRead() && this.blockInFd != null) {
			// Advise that this file descriptor will be accessed sequentially.
			NativeIO.posixFadviseIfPossible(this.blockInFd, 0, 0, NativeIO.POSIX_FADV_SEQUENTIAL);
		}

		// Trigger readahead of beginning of file if configured.
		manageOsCache();

		final long startTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
		try {
			int maxChunksPerPacket;
			int pktSize = PacketHeader.PKT_HEADER_LEN;
			final boolean transferTo = this.transferToAllowed && !this.verifyChecksum
				&& baseStream instanceof SocketOutputStream && this.blockIn instanceof FileInputStream;
			if (transferTo) {
				final FileChannel fileChannel = ((FileInputStream) this.blockIn).getChannel();
				this.blockInPosition = fileChannel.position();
				streamForSendChunks = baseStream;
				maxChunksPerPacket = numberOfChunks(TRANSFERTO_BUFFER_SIZE);

				// Smaller packet size to only hold checksum when doing transferTo
				pktSize += this.checksumSize * maxChunksPerPacket;
			} else {
				maxChunksPerPacket = Math.max(1,
					numberOfChunks(HdfsConstants.IO_FILE_BUFFER_SIZE));
				// Packet size includes both checksum and data
				pktSize += (this.chunkSize + this.checksumSize) * maxChunksPerPacket;
			}

			ByteBuffer pktBuf = ByteBuffer.allocate(pktSize);

			while (this.endOffset > this.offset) {
				manageOsCache();
				long len = sendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks,
					transferTo, throttler);
				this.offset += len;
				totalRead += len + (numberOfChunks(len) * this.checksumSize);
				this.seqno++;
			}
			try {
				// send an empty packet to mark the end of the block
				sendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks, transferTo,
					throttler);
				out.flush();
			} catch (IOException e) { // socket error
				throw ioeToSocketException(e);
			}

			this.sentEntireByteRange = true;
		} finally {
			if (this.clientTraceFmt != null) {
				final long endTime = System.nanoTime();
				ClientTraceLog.info(String.format(this.clientTraceFmt, totalRead, this.initialOffset, endTime
					- startTime));
			}
			close();
		}
		return totalRead;
	}

	/**
	 * Manage the OS buffer cache by performing read-ahead
	 * and drop-behind.
	 */
	private void manageOsCache() throws IOException {
		if (!isLongRead() || blockInFd == null) {
			// don't manage cache manually for short-reads, like
			// HBase random read workloads.
			return;
		}

		// Perform readahead if necessary
		if (readaheadLength > 0 && readaheadPool != null) {
			curReadahead = readaheadPool.readaheadStream(
				clientTraceFmt, blockInFd,
				offset, readaheadLength, Long.MAX_VALUE,
				curReadahead);
		}

		// Drop what we've just read from cache, since we aren't
		// likely to need it again
		long nextCacheDropOffset = lastCacheDropOffset + CACHE_DROP_INTERVAL_BYTES;
		if (shouldDropCacheBehindRead &&
			offset >= nextCacheDropOffset) {
			long dropLength = offset - lastCacheDropOffset;
			if (dropLength >= 1024) {
				NativeIO.posixFadviseIfPossible(blockInFd,
					lastCacheDropOffset, dropLength,
					NativeIO.POSIX_FADV_DONTNEED);
			}
			lastCacheDropOffset += CACHE_DROP_INTERVAL_BYTES;
		}
	}

	private boolean isLongRead() {
		return (endOffset - offset) > LONG_READ_THRESHOLD_BYTES;
	}

	/**
	 * Write packet header into {@code pkt}
	 */
	private void writePacketHeader(ByteBuffer pkt, int dataLen, int packetLen) {
		pkt.clear();
		PacketHeader header = new PacketHeader(packetLen, offset, seqno,
			(dataLen == 0), dataLen);
		header.putInBuffer(pkt);
	}

	boolean didSendEntireByteRange() {
		return sentEntireByteRange;
	}

	/**
	 * @return the checksum type that will be used with this block transfer.
	 */
	DataChecksum getChecksum() {
		return checksum;
	}

	/**
	 * @return the offset into the block file where the sender is currently
	 *         reading.
	 */
	long getOffset() {
		return offset;
	}
}
