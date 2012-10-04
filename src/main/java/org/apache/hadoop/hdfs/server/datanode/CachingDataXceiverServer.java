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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;

/**
 * Server used for receiving/sending a block of data.
 * This is created to listen for requests from clients or
 * other DataNodes. This small server does not use the
 * Hadoop IPC mechanism.
 */
class CachingDataXceiverServer extends DataXceiverServer {

	private final BlockCache blockCache;

	CachingDataXceiverServer(final ServerSocket ss, final Configuration conf, final DataNode datanode,
			final BlockCache blockCache) {

		super(ss, conf, datanode);

		this.blockCache = blockCache;
	}

	@Override
	public void run() {

		while (datanode.shouldRun) {
			Socket s = null;
			try {
				s = ss.accept();
				s.setTcpNoDelay(true);
				// Timeouts are set within DataXceiver.run()

				// Make sure the xceiver count is not exceeded
				int curXceiverCount = datanode.getXceiverCount();
				if (curXceiverCount > maxXceiverCount) {
					throw new IOException("Xceiver count " + curXceiverCount
						+ " exceeds the limit of concurrent xcievers: "
						+ maxXceiverCount);
				}

				new Daemon(datanode.threadGroup,
					CachingDataXceiver.create(s, datanode, this, this.blockCache))
					.start();
			} catch (SocketTimeoutException ignored) {
				// wake up to see if should continue to run
			} catch (AsynchronousCloseException ace) {
				// another thread closed our listener socket - that's expected during shutdown,
				// but not in other circumstances
				if (datanode.shouldRun) {
					LOG.warn(datanode.getDisplayName() + ":DataXceiverServer: ", ace);
				}
			} catch (IOException ie) {
				IOUtils.closeSocket(s);
				LOG.warn(datanode.getDisplayName() + ":DataXceiverServer: ", ie);
			} catch (OutOfMemoryError ie) {
				IOUtils.closeSocket(s);
				// DataNode can run out of memory if there is too many transfers.
				// Log the event, Sleep for 30 seconds, other transfers may complete by
				// then.
				LOG.warn("DataNode is out of memory. Will retry in 30 seconds.", ie);
				try {
					Thread.sleep(30 * 1000);
				} catch (InterruptedException e) {
					// ignore
				}
			} catch (Throwable te) {
				LOG.error(datanode.getDisplayName()
					+ ":DataXceiverServer: Exiting due to: ", te);
				datanode.shouldRun = false;
			}
		}
		try {
			ss.close();
		} catch (IOException ie) {
			LOG.warn(datanode.getDisplayName()
				+ " :DataXceiverServer: close exception", ie);
		}
	}
}
