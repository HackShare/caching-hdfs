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
import java.util.ArrayDeque;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.berkeley.icsi.memngt.protocols.DaemonToClientProtocol;
import edu.berkeley.icsi.memngt.utils.ClientUtils;

public final class BlockCache implements DaemonToClientProtocol {

	private static final Log LOG = LogFactory.getLog(BlockCache.class);

	private static final int PAGE_SIZE = 32 * 1024;

	private static final int ADAPTATION_GRANULARITY = 1024;

	private final ArrayDeque<byte[]> freeSegments = new ArrayDeque<byte[]>();

	private final int safetyMargin;

	private final int pid;

	private int grantedMemoryShare;

	BlockCache(final int grantedMemoryShare) {

		this.pid = ClientUtils.getPID();
		this.safetyMargin = ClientUtils.getMinHeapFreeRatio();

		this.grantedMemoryShare = grantedMemoryShare;

		synchronized (this) {
			adaptMemoryResources();
		}
	}

	/**
	 * Adapts the resources of the memory manager according to the granted memory share of
	 * the memory negotiator daemon.
	 */
	private void adaptMemoryResources() {

		LOG.debug("Adapting memory resources");

		int added = 0;
		while (ClientUtils.getPhysicalMemorySize(this.pid) < this.grantedMemoryShare) {
			this.freeSegments.add(new byte[PAGE_SIZE]);
			++added;
		}

		int removed = 0;
		while (ClientUtils.getPhysicalMemorySize(this.pid) > this.grantedMemoryShare) {
			for (int i = 0; i < ADAPTATION_GRANULARITY; ++i) {
				this.freeSegments.poll();
				++removed;

				// No more free segments to relinquish
				if (this.freeSegments.isEmpty()) {
					break;
				}
			}

			// No more free segments to relinquish
			if (this.freeSegments.isEmpty()) {
				break;
			}

			System.gc();
		}

		if (ClientUtils.getPhysicalMemorySize(this.pid) > this.grantedMemoryShare) {
			LOG.info("Need to relinquish more memory ");
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Physical memory size is " + ClientUtils.getPhysicalMemorySize(this.pid) +
				", granted memory share is " + this.grantedMemoryShare + " (added " + added +
				" memory segments, reliquished " + removed + ", now " + this.freeSegments.size()
				+ " free pages are available)");
		}

		ClientUtils.dumpMemoryUtilization();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void grantedMemoryShareChanged(final int sizeOfNewGrantedShare) throws IOException {

		this.grantedMemoryShare = sizeOfNewGrantedShare;
		adaptMemoryResources();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized int additionalMemoryOffered(final int amountOfAdditionalMemory) throws IOException {

		int maxUsableMem = ClientUtils.getMaximumUsableMemory();
		if (maxUsableMem == -1) {
			LOG.error("Cannot determine maximum amount of usable memory");
			return 0;
		}

		// Calculate the amount of additional memory that is actually usable
		maxUsableMem = Math.round((float) maxUsableMem * (float) (100 - this.safetyMargin) / 100.0f);
		maxUsableMem -= ClientUtils.getPhysicalMemorySize(this.pid);

		System.out.println("Offered " + amountOfAdditionalMemory + ", usable " + maxUsableMem);

		maxUsableMem = Math.min(maxUsableMem, amountOfAdditionalMemory);
		if (maxUsableMem < (PAGE_SIZE * ADAPTATION_GRANULARITY / 1024)) {
			LOG.info("Amount of additionally usable memory is below adaptation granularity, rejecting offer...");
			return 0;
		}

		this.grantedMemoryShare += maxUsableMem;

		LOG.info("Accepting " + maxUsableMem + " kilobytes of additional memory");

		adaptMemoryResources();

		return maxUsableMem;
	}
}
