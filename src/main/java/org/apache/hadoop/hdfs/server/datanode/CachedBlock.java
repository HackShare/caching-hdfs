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

import java.util.List;

/**
 * This class represents an HDFS block which is cached in memory.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
final class CachedBlock {

	/**
	 * The list buffer containing the cached data
	 */
	private final List<byte[]> cachedBuffers;

	/**
	 * The overall size of the cached data in bytes.
	 */
	private final int length;

	CachedBlock(final List<byte[]> cachedBuffers, final int length) {
		this.cachedBuffers = cachedBuffers;
		this.length = length;
	}

	/**
	 * Returns the list of buffers containing the cached data.
	 * 
	 * @return the list of buffers containing the cached data
	 */
	List<byte[]> getCachedBuffers() {
		return this.cachedBuffers;
	}

	/**
	 * The overall size of the cached data in bytes.
	 * 
	 * @return the overall size of the cached data in bytes
	 */
	int getLength() {
		return this.length;
	}
}
