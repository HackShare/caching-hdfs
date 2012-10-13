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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

import edu.berkeley.icsi.memngt.pools.AbstractMemoryPool;
import edu.berkeley.icsi.memngt.utils.ClientUtils;

public final class BlockCache extends AbstractMemoryPool<byte[]> {

	private static final class CachedBlockEntry {

		private final CachedBlock cachedBlock;

		private int lockCounter = 0;

		private CachedBlockEntry(final CachedBlock cachedBlock) {
			this.cachedBlock = cachedBlock;
		}

	}

	private final int pageSize;

	private final Map<ExtendedBlock, CachedBlockEntry> cachedBlocks = new HashMap<ExtendedBlock, CachedBlockEntry>();

	BlockCache(int pageSize) {
		super("DataNode Memory Pool", calculatePoolCapacity(pageSize / 1024), pageSize / 1024);
		this.pageSize = pageSize;
	}

	private static int calculatePoolCapacity(final int pageSize) {

		return ClientUtils.getMaximumUsableMemory() / pageSize;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected byte[] allocatedNewBuffer() {
		return new byte[this.pageSize];
	}

	CachedBlock lock(final ExtendedBlock block) {

		synchronized (this) {

			final CachedBlockEntry entry = this.cachedBlocks.get(block);
			if (entry == null) {
				return null;
			}

			entry.lockCounter++;

			return entry.cachedBlock;
		}
	}

	void unlock(final ExtendedBlock block) {

		synchronized (this) {

			final CachedBlockEntry entry = this.cachedBlocks.get(block);
			if (entry == null) {
				throw new IllegalStateException("Cannot find entry for block " + block);
			}

			entry.lockCounter--;

			if (entry.lockCounter < 0) {
				throw new IllegalStateException("Lock counter for block " + block + " block is " + entry.lockCounter);
			}
		}
	}

	void addCachedBlock(final ExtendedBlock block, final CachedBlock cachedBlock) {

		synchronized (this) {

			if (this.cachedBlocks.containsKey(block)) {
				// Another has already been added for the same block in the meantime
				destroyCachedBlock(cachedBlock);
				return;
			}

			this.cachedBlocks.put(block, new CachedBlockEntry(cachedBlock));
		}

	}

	private void destroyCachedBlock(final CachedBlock cachedBlock) {

		final Iterator<byte[]> it = cachedBlock.getCachedBuffers().iterator();
		while (it.hasNext()) {
			returnBuffer(it.next());
		}
	}
}
