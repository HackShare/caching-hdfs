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
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

import edu.berkeley.icsi.memngt.pools.AbstractMemoryPool;
import edu.berkeley.icsi.memngt.utils.ClientUtils;

public final class BlockCache extends AbstractMemoryPool<byte[]> {

	private final int pageSize;

	private final Map<ExtendedBlock, Object> cachedBlocks = new HashMap<ExtendedBlock, Object>();

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

	boolean lock(final ExtendedBlock block) {

		return this.cachedBlocks.containsKey(block);
	}
}
