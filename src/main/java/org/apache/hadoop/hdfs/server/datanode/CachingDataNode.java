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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.channels.ServerSocketChannel;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ClientDatanodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.InterDatanodeProtocolProtos.InterDatanodeProtocolService;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.InterDatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.InterDatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.log4j.BasicConfigurator;

import com.google.protobuf.BlockingService;

import edu.berkeley.icsi.memngt.protocols.ClientToDaemonProtocol;
import edu.berkeley.icsi.memngt.protocols.DaemonToClientProtocol;
import edu.berkeley.icsi.memngt.protocols.NegotiationException;
import edu.berkeley.icsi.memngt.protocols.ProcessType;
import edu.berkeley.icsi.memngt.rpc.RPCService;
import edu.berkeley.icsi.memngt.utils.ClientUtils;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_STARTUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT;

public final class CachingDataNode implements ClientDatanodeProtocol, InterDatanodeProtocol, DaemonToClientProtocol {

	/**
	 * The port the RPC service of the local memory negotiator listens on.
	 */
	private static final int MEMORY_NEGOTIATOR_RPC_PORT = 8010;

	public static final Log LOG = LogFactory.getLog(CachingDataNode.class);

	private final DataNode dataNode;

	private final DNConf dnConf;

	private final BlockCache blockCache;

	private final RPCService rpcService;

	static {
		HdfsConfiguration.init();
	}

	private CachingDataNode(final Configuration conf, final AbstractList<File> dataDirs, final SecureResources resources)
			throws IOException {

		this.dataNode = new DataNode(conf, dataDirs, resources);
		this.dnConf = new DNConf(conf);

		// initialize the RPC connection to the memory
		this.rpcService = new RPCService(MEMORY_NEGOTIATOR_RPC_PORT);
		this.rpcService.setProtocolCallbackHandler(DaemonToClientProtocol.class, this);

		ClientToDaemonProtocol proxy = this.rpcService.getProxy(new InetSocketAddress(8009),
			ClientToDaemonProtocol.class);

		int grantedMemoryShare;
		try {
			grantedMemoryShare = proxy.registerClient("Caching HDFS", ClientUtils.getPID(),
				this.rpcService.getRPCPort(),
				ProcessType.INFRASTRUCTURE_PROCESS);
		} catch (NegotiationException ne) {
			LOG.error(ne);
			throw new IOException(ne);
		}

		this.blockCache = new BlockCache(grantedMemoryShare);

		reconfigureIpcServer(conf);
		reconfigureDataXceiver(conf);
	}

	private void reconfigureIpcServer(final Configuration conf) throws IOException {

		this.dataNode.ipcServer.stop();

		final InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
			conf.get(DFS_DATANODE_IPC_ADDRESS_KEY));

		// Add all the RPC protocols that the Datanode implements
		RPC.setProtocolEngine(conf, ClientDatanodeProtocolPB.class,
			ProtobufRpcEngine.class);
		final ClientDatanodeProtocolServerSideTranslatorPB clientDatanodeProtocolXlator =
			new ClientDatanodeProtocolServerSideTranslatorPB(this);
		BlockingService service = ClientDatanodeProtocolService
			.newReflectiveBlockingService(clientDatanodeProtocolXlator);

		final boolean oldVal = DefaultMetricsSystem.inMiniClusterMode();
		DefaultMetricsSystem.setMiniClusterMode(true);
		this.dataNode.ipcServer = RPC.getServer(ClientDatanodeProtocolPB.class, service, ipcAddr
			.getHostName(), ipcAddr.getPort(), conf.getInt(
			DFS_DATANODE_HANDLER_COUNT_KEY, DFS_DATANODE_HANDLER_COUNT_DEFAULT),
			false, conf, this.dataNode.blockPoolTokenSecretManager);
		DefaultMetricsSystem.setMiniClusterMode(oldVal);

		final InterDatanodeProtocolServerSideTranslatorPB interDatanodeProtocolXlator =
			new InterDatanodeProtocolServerSideTranslatorPB(this);
		service = InterDatanodeProtocolService
			.newReflectiveBlockingService(interDatanodeProtocolXlator);
		DFSUtil.addPBProtocol(conf, InterDatanodeProtocolPB.class, service,
			this.dataNode.ipcServer);
		LOG.info("Opened IPC server at " + this.dataNode.ipcServer.getListenerAddress());

		// set service-level authorization security policy
		if (conf.getBoolean(
			CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
			this.dataNode.ipcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
		}
	}

	private void reconfigureDataXceiver(final Configuration conf) throws IOException {

		final ServerSocket ss = ((DataXceiverServer) this.dataNode.dataXceiverServer.getRunnable()).ss;

		// Shut down old dataXceiverServer and replace it with our version
		if (this.dataNode.dataXceiverServer != null) {
			((DataXceiverServer) this.dataNode.dataXceiverServer.getRunnable()).kill();
			this.dataNode.dataXceiverServer.interrupt();

			// wait for all data receiver threads to exit
			if (this.dataNode.threadGroup != null) {
				int sleepMs = 2;
				while (true) {
					this.dataNode.threadGroup.interrupt();
					LOG.info("Waiting for threadgroup to exit, active threads is " +
						this.dataNode.threadGroup.activeCount());
					if (this.dataNode.threadGroup.activeCount() == 0) {
						break;
					}
					try {
						Thread.sleep(sleepMs);
					} catch (InterruptedException e) {
					}
					sleepMs = sleepMs * 3 / 2; // exponential backoff
					if (sleepMs > 1000) {
						sleepMs = 1000;
					}
				}
			}
			// wait for dataXceiveServer to terminate
			try {
				this.dataNode.dataXceiverServer.join();
			} catch (InterruptedException ie) {
			}
		}

		// find free port or use privileged port provided
		final ServerSocket newServerSocket = (dnConf.socketWriteTimeout > 0) ?
			ServerSocketChannel.open().socket() : new ServerSocket();
		newServerSocket.setReceiveBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
		Server.bind(newServerSocket, (InetSocketAddress) ss.getLocalSocketAddress(), 0);
		newServerSocket.setReceiveBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);

		this.dataNode.threadGroup = new ThreadGroup("cachingDataXceiverServer");
		this.dataNode.dataXceiverServer = new Daemon(this.dataNode.threadGroup,
			new CachingDataXceiverServer(newServerSocket, conf, this.dataNode, this.blockCache));
		this.dataNode.threadGroup.setDaemon(true); // auto destroy when empty
	}

	public static void main(final String args[]) {

		BasicConfigurator.configure();
		final Configuration conf = new Configuration();
		conf.set("dfs.namenode.rpc-address", "localhost:8000");

		secureMain(args, null, conf);
	}

	public static void secureMain(final String args[], final SecureResources resources, final Configuration conf) {

		try {
			StringUtils.startupShutdownMessage(DataNode.class, args, LOG);
			final CachingDataNode datanode = createCachingDataNode(args, conf, resources);
			if (datanode != null)
				datanode.join();
		} catch (Throwable e) {
			LOG.error("Exception in secureMain", e);
			System.exit(-1);
		} finally {
			// We need to add System.exit here because either shutdown was called or
			// some disk related conditions like volumes tolerated or volumes required
			// condition was not met. Also, In secure mode, control will go to Jsvc
			// and Datanode process hangs without System.exit.
			LOG.warn("Exiting Datanode");
			System.exit(0);
		}
	}

	public void join() {

		try {
			this.dataNode.join();
		} finally {
			this.rpcService.shutDown();
		}
	}

	/**
	 * Instantiate & Start a single datanode daemon and wait for it to finish.
	 * If this thread is specifically interrupted, it will stop waiting.
	 */
	public static CachingDataNode createCachingDataNode(final String args[], final Configuration conf,
			final SecureResources resources)
			throws IOException {

		final CachingDataNode cdn = instantiateCachingDataNode(args, conf, resources);
		if (cdn != null) {
			cdn.runDatanodeDaemon();
		}
		return cdn;
	}

	/**
	 * Start a single datanode daemon and wait for it to finish.
	 * If this thread is specifically interrupted, it will stop waiting.
	 */
	public void runDatanodeDaemon() throws IOException {
		this.dataNode.runDatanodeDaemon();
	}

	/**
	 * Instantiate a single datanode object, along with its secure resources.
	 * This must be run by invoking{@link DataNode#runDatanodeDaemon()} subsequently.
	 */
	public static CachingDataNode instantiateCachingDataNode(String args[], Configuration conf,
			final SecureResources resources) throws IOException {

		if (conf == null) {
			conf = new HdfsConfiguration();
		}

		if (args != null) {
			// parse generic hadoop options
			final GenericOptionsParser hParser = new GenericOptionsParser(conf, args);
			args = hParser.getRemainingArgs();
		}

		if (!parseArguments(args, conf)) {
			printUsage();
			return null;
		}

		final Collection<URI> dataDirs = getStorageDirs(conf);
		UserGroupInformation.setConfiguration(conf);
		SecurityUtil.login(conf, DFS_DATANODE_KEYTAB_FILE_KEY,
			DFS_DATANODE_USER_NAME_KEY);
		return makeInstance(dataDirs, conf, resources);
	}

	static Collection<URI> getStorageDirs(final Configuration conf) {

		final Collection<String> dirNames = conf.getTrimmedStringCollection(DFS_DATANODE_DATA_DIR_KEY);
		return Util.stringCollectionAsURIs(dirNames);
	}

	private static void printUsage() {
		System.err.println("Usage: java DataNode");
		System.err.println("           [-rollback]");
	}

	/**
	 * Parse and verify command line arguments and set configuration parameters.
	 * 
	 * @return false if passed arguments are incorrect
	 */
	private static boolean parseArguments(final String args[], final Configuration conf) {

		int argsLen = (args == null) ? 0 : args.length;

		StartupOption startOpt = StartupOption.REGULAR;

		for (int i = 0; i < argsLen; i++) {
			String cmd = args[i];
			if ("-r".equalsIgnoreCase(cmd) || "--rack".equalsIgnoreCase(cmd)) {
				LOG.error("-r, --rack arguments are not supported anymore. RackID " +
					"resolution is handled by the NameNode.");
				System.exit(-1);
			} else if ("-rollback".equalsIgnoreCase(cmd)) {
				startOpt = StartupOption.ROLLBACK;
			} else if ("-regular".equalsIgnoreCase(cmd)) {
				startOpt = StartupOption.REGULAR;
			} else
				return false;
		}
		setStartupOption(conf, startOpt);
		return true;
	}

	private static void setStartupOption(final Configuration conf, final StartupOption opt) {
		conf.set(DFS_DATANODE_STARTUP_KEY, opt.toString());
	}

	/**
	 * Make an instance of DataNode after ensuring that at least one of the
	 * given data directories (and their parent directories, if necessary)
	 * can be created.
	 * 
	 * @param dataDirs
	 *        List of directories, where the new DataNode instance should
	 *        keep its files.
	 * @param conf
	 *        Configuration instance to use.
	 * @param resources
	 *        Secure resources needed to run under Kerberos
	 * @return DataNode instance for given list of data dirs and conf, or null if
	 *         no directory from this directory list can be created.
	 * @throws IOException
	 */
	static CachingDataNode makeInstance(final Collection<URI> dataDirs, final Configuration conf,
			final SecureResources resources) throws IOException {

		final LocalFileSystem localFS = FileSystem.getLocal(conf);
		final FsPermission permission = new FsPermission(conf.get(DFS_DATANODE_DATA_DIR_PERMISSION_KEY,
			DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT));

		final ArrayList<File> dirs = getDataDirsFromURIs(dataDirs, localFS, permission);
		DefaultMetricsSystem.initialize("DataNode");

		assert dirs.size() > 0 : "number of data directories should be > 0";
		return new CachingDataNode(conf, dirs, resources);
	}

	// DataNode ctor expects AbstractList instead of List or Collection...
	static ArrayList<File> getDataDirsFromURIs(final Collection<URI> dataDirs, final LocalFileSystem localFS,
			final FsPermission permission) throws IOException {

		final ArrayList<File> dirs = new ArrayList<File>();
		final StringBuilder invalidDirs = new StringBuilder();

		for (final URI dirURI : dataDirs) {
			if (!"file".equalsIgnoreCase(dirURI.getScheme())) {
				LOG.warn("Unsupported URI schema in " + dirURI + ". Ignoring ...");
				invalidDirs.append("\"").append(dirURI).append("\" ");
				continue;
			}

			// drop any (illegal) authority in the URI for backwards compatibility
			final File dir = new File(dirURI.getPath());
			try {
				DiskChecker.checkDir(localFS, new Path(dir.toURI()), permission);
				dirs.add(dir);
			} catch (IOException ioe) {
				LOG.warn("Invalid " + DFS_DATANODE_DATA_DIR_KEY + " "
					+ dir + " : ", ioe);
				invalidDirs.append("\"").append(dir.getCanonicalPath()).append("\" ");
			}
		}

		if (dirs.size() == 0) {
			throw new IOException("All directories in "
				+ DFS_DATANODE_DATA_DIR_KEY + " are invalid: "
				+ invalidDirs);
		}

		return dirs;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getReplicaVisibleLength(final ExtendedBlock b) throws IOException {

		LOG.warn("CALL");

		System.out.println("Call");

		return this.dataNode.getReplicaVisibleLength(b);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void refreshNamenodes() throws IOException {

		LOG.warn("CALL");

		this.dataNode.refreshNamenodes();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteBlockPool(final String bpid, final boolean force) throws IOException {

		LOG.warn("CALL");

		this.dataNode.deleteBlockPool(bpid, force);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BlockLocalPathInfo getBlockLocalPathInfo(final ExtendedBlock block, final Token<BlockTokenIdentifier> token)
			throws IOException {

		LOG.warn("CALL");

		return this.dataNode.getBlockLocalPathInfo(block, token);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ReplicaRecoveryInfo initReplicaRecovery(final RecoveringBlock rBlock) throws IOException {

		LOG.warn("CALL");

		return this.dataNode.initReplicaRecovery(rBlock);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String updateReplicaUnderRecovery(final ExtendedBlock oldBlock, final long recoveryId, final long newLength)
			throws IOException {

		LOG.warn("CALL");

		return this.dataNode.updateReplicaUnderRecovery(oldBlock, recoveryId, newLength);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void grantedMemoryShareChanged(final int sizeOfNewGrantedShare) throws IOException {

		this.blockCache.additionalMemoryOffered(sizeOfNewGrantedShare);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int additionalMemoryOffered(final int amountOfAdditionalMemory) throws IOException {

		return this.blockCache.additionalMemoryOffered(amountOfAdditionalMemory);
	}
}