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
package org.apache.zookeeper.server.quorum;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.sasl.SaslException;

import org.apache.zookeeper.KeeperException.BadArgumentsException;
import org.apache.zookeeper.common.AtomicFileWritingIdiom;
import org.apache.zookeeper.common.AtomicFileWritingIdiom.WriterStatement;
import org.apache.zookeeper.common.QuorumX509Util;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.jmx.ZKMBeanInfo;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.auth.QuorumAuth;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.quorum.auth.SaslQuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.SaslQuorumAuthServer;
import org.apache.zookeeper.server.quorum.auth.NullQuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.NullQuorumAuthServer;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.admin.AdminServerFactory;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the quorum protocol. There are three states this server
 * can be in:
 * <ol>
 * <li>Leader election - each server will elect a leader (proposing itself as a
 * leader initially).</li>
 * <li>Follower - the server will synchronize with the leader and replicate any
 * transactions.</li>
 * <li>Leader - the server will process requests and forward them to followers.
 * A majority of followers must log the request before it can be accepted.
 * </ol>
 *
 * This class will setup a datagram socket that will always respond with its
 * view of the current leader. The response will take the form of:
 *
 * <pre>
 * int xid;
 *
 * long myid;
 *
 * long leader_id;
 *
 * long leader_zxid;
 * </pre>
 *
 * The request for the current leader will consist solely of an xid: int xid;
 */
public class QuorumPeer extends ZooKeeperThread implements QuorumStats.Provider {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeer.class);

    private QuorumBean jmxQuorumBean;
    LocalPeerBean jmxLocalPeerBean;
    private Map<Long, RemotePeerBean> jmxRemotePeerBean;
    LeaderElectionBean jmxLeaderElectionBean;

    // The QuorumCnxManager is held through an AtomicReference to ensure cross-thread visibility
    // of updates; see the implementation comment at setLastSeenQuorumVerifier().
    private AtomicReference<QuorumCnxManager> qcmRef = new AtomicReference<>();

    QuorumAuthServer authServer;
    QuorumAuthLearner authLearner;

    /**
     * ZKDatabase is a top level member of quorumpeer
     * which will be used in all the zookeeperservers
     * instantiated later. Also, it is created once on
     * bootup and only thrown away in case of a truncate
     * message from the leader
     */
    private ZKDatabase zkDb;

    public static final class AddressTuple {
        public final InetSocketAddress quorumAddr;
        public final InetSocketAddress electionAddr;
        public final InetSocketAddress clientAddr;

        public AddressTuple(InetSocketAddress quorumAddr, InetSocketAddress electionAddr, InetSocketAddress clientAddr) {
            // 通信地址 server.1=name1:2888:3888
            // 此quorumAddr为name1:2888
            this.quorumAddr = quorumAddr;
            // 此electionAddr为name1:3888
            this.electionAddr = electionAddr;
            this.clientAddr = clientAddr;
        }
    }

    public static class QuorumServer {
        // server间通信地址
        //server.1=name2:2888:3888, 此addr=name2:2888
        public InetSocketAddress addr = null;
        // 选举地址 此地址为   name2:3888
        public InetSocketAddress electionAddr = null;
        
        public InetSocketAddress clientAddr = null;
        // 此server的sid
        public long id;
        // 此server的host地址
        public String hostname;
        // server创建时,默认的角色
        public LearnerType type = LearnerType.PARTICIPANT;
        // 记录解析到的地址,此地址中不包括自己的地址
        private List<InetSocketAddress> myAddrs;

        public QuorumServer(long id, InetSocketAddress addr,
                InetSocketAddress electionAddr, InetSocketAddress clientAddr) {
            this(id, addr, electionAddr, clientAddr, LearnerType.PARTICIPANT);
        }

        public QuorumServer(long id, InetSocketAddress addr,
                InetSocketAddress electionAddr) {
            this(id, addr, electionAddr, (InetSocketAddress)null, LearnerType.PARTICIPANT);
        }

        // VisibleForTesting
        public QuorumServer(long id, InetSocketAddress addr) {
            this(id, addr, (InetSocketAddress)null, (InetSocketAddress)null, LearnerType.PARTICIPANT);
        }
 
        public long getId() {
            return id;
        }

        /**
         * Performs a DNS lookup for server address and election address.
         *
         * If the DNS lookup fails, this.addr and electionAddr remain
         * unmodified.
         */
        public void recreateSocketAddresses() {
            if (this.addr == null) {
                LOG.warn("Server address has not been initialized");
                return;
            }
            if (this.electionAddr == null) {
                LOG.warn("Election address has not been initialized");
                return;
            }
            String host = this.addr.getHostString();
            InetAddress address = null;
            try {
                address = InetAddress.getByName(host);
            } catch (UnknownHostException ex) {
                LOG.warn("Failed to resolve address: {}", host, ex);
                return;
            }
            LOG.debug("Resolved address for {}: {}", host, address);
            int port = this.addr.getPort();
            this.addr = new InetSocketAddress(address, port);
            port = this.electionAddr.getPort();
            this.electionAddr = new InetSocketAddress(address, port);
        }

        private void setType(String s) throws ConfigException {
            if (s.toLowerCase().equals("observer")) {
               type = LearnerType.OBSERVER;
           } else if (s.toLowerCase().equals("participant")) {
               type = LearnerType.PARTICIPANT;
            } else {
               throw new ConfigException("Unrecognised peertype: " + s);
            }
        }

        private static String[] splitWithLeadingHostname(String s)
                throws ConfigException
        {
            /* Does it start with an IPv6 literal? */
            // ipv6地址的分析
            if (s.startsWith("[")) {
                int i = s.indexOf("]:");
                if (i < 0) {
                    throw new ConfigException(s + " starts with '[' but has no matching ']:'");
                }

                String[] sa = s.substring(i + 2).split(":");
                String[] nsa = new String[sa.length + 1];
                nsa[0] = s.substring(1, i);
                System.arraycopy(sa, 0, nsa, 1, sa.length);

                return nsa;
            } else {
                // 普通地址 使用 : 分隔
                return s.split(":");
            }
        }

        private static final String wrongFormat = " does not have the form server_config or server_config;client_config"+
        " where server_config is host:port:port or host:port:port:type and client_config is port or host:port";

        /**
         * todo  解析 server.1=10.0.0.0:2888:3888  配置的解析
         */
        public QuorumServer(long sid, String addressStr) throws ConfigException {
            // LOG.warn("sid = " + sid + " addressStr = " + addressStr);
            // 记录此server对应的myid
            this.id = sid;
            // 解析地址
            // 由此可见,当配置多个地址时,使用 ; 分割
            String serverClientParts[] = addressStr.split(";");
            // 多个地址 也会先使用第一个
            String serverParts[] = splitWithLeadingHostname(serverClientParts[0]);
            // 可见使用多个地址不能大于2
            // 地址的格式不正确也会报错
            if ((serverClientParts.length > 2) || (serverParts.length < 3)
                    || (serverParts.length > 4)) {
                throw new ConfigException(addressStr + wrongFormat);
            }

            if (serverClientParts.length == 2) {
                //LOG.warn("ClientParts: " + serverClientParts[1]);
                String clientParts[] = splitWithLeadingHostname(serverClientParts[1]);
                if (clientParts.length > 2) {
                    throw new ConfigException(addressStr + wrongFormat);
                }

                // is client_config a host:port or just a port
                hostname = (clientParts.length == 2) ? clientParts[0] : "0.0.0.0";
                try {
                    // client地址 todo  不是很清楚此clientAddr的作用
                    clientAddr = new InetSocketAddress(hostname,
                            Integer.parseInt(clientParts[clientParts.length - 1]));
                    //LOG.warn("Set clientAddr to " + clientAddr);
                } catch (NumberFormatException e) {
                    throw new ConfigException("Address unresolved: " + hostname + ":" + clientParts[clientParts.length - 1]);
                }
            }

            // server_config should be either host:port:port or host:port:port:type
            try {
                //server.1=name2:2888:3888, 此addr=name2:2888
                addr = new InetSocketAddress(serverParts[0],
                        Integer.parseInt(serverParts[1]));
            } catch (NumberFormatException e) {
                throw new ConfigException("Address unresolved: " + serverParts[0] + ":" + serverParts[1]);
            }
            try {
                // 选举地址 server.1=name2:2888:3888, 此addr=name2:3888
                electionAddr = new InetSocketAddress(serverParts[0],
                        Integer.parseInt(serverParts[2]));
            } catch (NumberFormatException e) {
                throw new ConfigException("Address unresolved: " + serverParts[0] + ":" + serverParts[2]);
            }
            // 通信端口和 选举端口一样,则报错
            if(addr.getPort() == electionAddr.getPort()) {
                throw new ConfigException(
                        "Client and election port must be different! Please update the configuration file on server." + sid);
            }
            // 类型, observer follower角色
            // 可见,有此配置 server.1=name2:2888:3888:observer
            // 可见,有此配置 server.1=name2:2888:3888:participant
            // 间接设置 角色
            if (serverParts.length == 4) {
                setType(serverParts[3]);
            }
            // 此server的地址
            this.hostname = serverParts[0];
            // 把解析的地址记录起来, 并去除自己的地址
            setMyAddrs();
        }

        public QuorumServer(long id, InetSocketAddress addr,
                    InetSocketAddress electionAddr, LearnerType type) {
            this(id, addr, electionAddr, (InetSocketAddress)null, type);
        }

        public QuorumServer(long id, InetSocketAddress addr,
                InetSocketAddress electionAddr, InetSocketAddress clientAddr, LearnerType type) {
            this.id = id;
            this.addr = addr;
            this.electionAddr = electionAddr;
            this.type = type;
            this.clientAddr = clientAddr;
            // 把解析的地址记录起来, 并去除自己的地址
            setMyAddrs();
        }

        private void setMyAddrs() {
            this.myAddrs = new ArrayList<InetSocketAddress>();
            this.myAddrs.add(this.addr);
            this.myAddrs.add(this.clientAddr);
            this.myAddrs.add(this.electionAddr);
            // 去除自己的地址
            this.myAddrs = excludedSpecialAddresses(this.myAddrs);
        }

        private static String delimitedHostString(InetSocketAddress addr)
        {
            String host = addr.getHostString();
            if (host.contains(":")) {
                return "[" + host + "]";
            } else {
                return host;
            }
        }

        public String toString(){
            StringWriter sw = new StringWriter();
            //addr should never be null, but just to make sure
            if (addr !=null) {
                sw.append(delimitedHostString(addr));
                sw.append(":");
                sw.append(String.valueOf(addr.getPort()));
            }
            if (electionAddr!=null){
                sw.append(":");
                sw.append(String.valueOf(electionAddr.getPort()));
            }           
            if (type == LearnerType.OBSERVER) sw.append(":observer");
            else if (type == LearnerType.PARTICIPANT) sw.append(":participant");            
            if (clientAddr!=null){
                sw.append(";");
                sw.append(delimitedHostString(clientAddr));
                sw.append(":");
                sw.append(String.valueOf(clientAddr.getPort()));
            }
            return sw.toString();       
        }

        public int hashCode() {
          assert false : "hashCode not designed";
          return 42; // any arbitrary constant will do 
        }
        
        private boolean checkAddressesEqual(InetSocketAddress addr1, InetSocketAddress addr2){
            if ((addr1 == null && addr2!=null) ||
                (addr1!=null && addr2==null) ||
                (addr1!=null && addr2!=null && !addr1.equals(addr2))) return false;
            return true;
        }
        
        public boolean equals(Object o){
            if (!(o instanceof QuorumServer)) return false;
            QuorumServer qs = (QuorumServer)o;          
            if ((qs.id != id) || (qs.type != type)) return false;   
            if (!checkAddressesEqual(addr, qs.addr)) return false;
            if (!checkAddressesEqual(electionAddr, qs.electionAddr)) return false;
            if (!checkAddressesEqual(clientAddr, qs.clientAddr)) return false;                    
            return true;
        }

        public void checkAddressDuplicate(QuorumServer s) throws BadArgumentsException {
            List<InetSocketAddress> otherAddrs = new ArrayList<InetSocketAddress>();
            otherAddrs.add(s.addr);
            otherAddrs.add(s.clientAddr);
            otherAddrs.add(s.electionAddr);
            otherAddrs = excludedSpecialAddresses(otherAddrs);

            for (InetSocketAddress my: this.myAddrs) {

                for (InetSocketAddress other: otherAddrs) {
                    if (my.equals(other)) {
                        String error = String.format("%s of server.%d conflicts %s of server.%d", my, this.id, other, s.id);
                        throw new BadArgumentsException(error);
                    }
                }
            }
        }

        private List<InetSocketAddress> excludedSpecialAddresses(List<InetSocketAddress> addrs) {
            // 记录最终的有效地址
            List<InetSocketAddress> included = new ArrayList<InetSocketAddress>();
            // 获取本地的地址
            InetAddress wcAddr = new InetSocketAddress(0).getAddress();
            // 遍历传递进来的所有地址, 来判断获取最终的有效地址
            for (InetSocketAddress addr : addrs) {
                // 跳过地址为空的
                if (addr == null) {
                    continue;
                }
                InetAddress inetaddr = addr.getAddress();
                // 地址为空 跳过
                // 地址等于自己的本地地址  跳过
                // 是 loopback地址, 跳过
                if (inetaddr == null ||
                    inetaddr.equals(wcAddr) || // wildCard address(0.0.0.0)
                    inetaddr.isLoopbackAddress()) { // loopback address(localhost/127.0.0.1)
                    continue;
                }
                // 记录有效地址
                included.add(addr);
            }
            // 最终返回所有的有效地址
            return included;
        }
    }


    public enum ServerState {
        LOOKING, FOLLOWING, LEADING, OBSERVING;
    }

    /*
     * A peer can either be participating, which implies that it is willing to
     * both vote in instances of consensus and to elect or become a Leader, or
     * it may be observing in which case it isn't.
     *
     * We need this distinction to decide which ServerState to move to when
     * conditions change (e.g. which state to become after LOOKING).
     */
    public enum LearnerType {
        PARTICIPANT, OBSERVER;
    }

    /*
     * To enable observers to have no identifier, we need a generic identifier
     * at least for QuorumCnxManager. We use the following constant to as the
     * value of such a generic identifier.
     */

    static final long OBSERVER_ID = Long.MAX_VALUE;

    /*
     * Record leader election time
     */
    public long start_fle, end_fle; // fle = fast leader election
    public static final String FLE_TIME_UNIT = "MS";

    /*
     * Default value of peer is participant
     */
    private LearnerType learnerType = LearnerType.PARTICIPANT;

    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * Sets the LearnerType
     */
    public void setLearnerType(LearnerType p) {
        learnerType = p;
    }

    protected synchronized void setConfigFileName(String s) {
        configFilename = s;
    }

    private String configFilename = null;

    public int getQuorumSize(){
        return getVotingView().size();
    }

    /**
     * QuorumVerifier implementation; default (majority).
     */

    //last committed quorum verifier
    private QuorumVerifier quorumVerifier;
    
    //last proposed quorum verifier
    private QuorumVerifier lastSeenQuorumVerifier = null;

    // Lock object that guard access to quorumVerifier and lastSeenQuorumVerifier.
    final Object QV_LOCK = new Object();


    /**
     * My id
     */
    private long myid;


    /**
     * get the id of this quorum peer.
     */
    public long getId() {
        return myid;
    }

    // VisibleForTesting
    void setId(long id) {
        this.myid = id;
    }

    private boolean sslQuorum;
    private boolean shouldUsePortUnification;

    public boolean isSslQuorum() {
        return sslQuorum;
    }

    public boolean shouldUsePortUnification() {
        return shouldUsePortUnification;
    }

    private final QuorumX509Util x509Util;

    QuorumX509Util getX509Util() {
        return x509Util;
    }

    /**
     * This is who I think the leader currently is.
     */
    volatile private Vote currentVote;

    public synchronized Vote getCurrentVote(){
        return currentVote;
    }

    public synchronized void setCurrentVote(Vote v){
        currentVote = v;
    }

    private volatile boolean running = true;

    /**
     * The number of milliseconds of each tick
     */
    protected int tickTime;

    /**
     * Whether learners in this quorum should create new sessions as local.
     * False by default to preserve existing behavior.
     */
    protected boolean localSessionsEnabled = false;

    /**
     * Whether learners in this quorum should upgrade local sessions to
     * global. Only matters if local sessions are enabled.
     */
    protected boolean localSessionsUpgradingEnabled = true;

    /**
     * Minimum number of milliseconds to allow for session timeout.
     * A value of -1 indicates unset, use default.
     */
    protected int minSessionTimeout = -1;

    /**
     * Maximum number of milliseconds to allow for session timeout.
     * A value of -1 indicates unset, use default.
     */
    protected int maxSessionTimeout = -1;

    /**
     * The number of ticks that the initial synchronization phase can take
     */
    protected int initLimit;

    /**
     * The number of ticks that can pass between sending a request and getting
     * an acknowledgment
     */
    protected int syncLimit;
    
    /**
     * Enables/Disables sync request processor. This option is enabled
     * by default and is to be used with observers.
     */
    protected boolean syncEnabled = true;

    /**
     * The current tick
     */
    protected AtomicInteger tick = new AtomicInteger();

    /**
     * Whether or not to listen on all IPs for the two quorum ports
     * (broadcast and fast leader election).
     */
    protected boolean quorumListenOnAllIPs = false;

    /**
     * Keeps time taken for leader election in milliseconds. Sets the value to
     * this variable only after the completion of leader election.
     */
    private long electionTimeTaken = -1;

    /**
     * Enable/Disables quorum authentication using sasl. Defaulting to false.
     */
    protected boolean quorumSaslEnableAuth;

    /**
     * If this is false, quorum peer server will accept another quorum peer client
     * connection even if the authentication did not succeed. This can be used while
     * upgrading ZooKeeper server. Defaulting to false (required).
     */
    protected boolean quorumServerSaslAuthRequired;

    /**
     * If this is false, quorum peer learner will talk to quorum peer server
     * without authentication. This can be used while upgrading ZooKeeper
     * server. Defaulting to false (required).
     */
    protected boolean quorumLearnerSaslAuthRequired;

    /**
     * Kerberos quorum service principal. Defaulting to 'zkquorum/localhost'.
     */
    protected String quorumServicePrincipal;

    /**
     * Quorum learner login context name in jaas-conf file to read the kerberos
     * security details. Defaulting to 'QuorumLearner'.
     */
    protected String quorumLearnerLoginContext;

    /**
     * Quorum server login context name in jaas-conf file to read the kerberos
     * security details. Defaulting to 'QuorumServer'.
     */
    protected String quorumServerLoginContext;

    // TODO: need to tune the default value of thread size
    private static final int QUORUM_CNXN_THREADS_SIZE_DEFAULT_VALUE = 20;
    /**
     * The maximum number of threads to allow in the connectionExecutors thread
     * pool which will be used to initiate quorum server connections.
     */
    protected int quorumCnxnThreadsSize = QUORUM_CNXN_THREADS_SIZE_DEFAULT_VALUE;

    /**
     * @deprecated As of release 3.4.0, this class has been deprecated, since
     * it is used with one of the udp-based versions of leader election, which
     * we are also deprecating.
     *
     * This class simply responds to requests for the current leader of this
     * node.
     * <p>
     * The request contains just an xid generated by the requestor.
     * <p>
     * The response has the xid, the id of this server, the id of the leader,
     * and the zxid of the leader.
     *
     *
     */
    @Deprecated
    class ResponderThread extends ZooKeeperThread {
        ResponderThread() {
            super("ResponderThread");
        }

        volatile boolean running = true;

        @Override
        public void run() {
            try {
                // 保存响应数据的缓存
                byte b[] = new byte[36];
                // 包装到buteBuffer
                ByteBuffer responseBuffer = ByteBuffer.wrap(b);
                DatagramPacket packet = new DatagramPacket(b, b.length);
                while (running) {
                    // 接收其他server 发送的packet
                    udpSocket.receive(packet);
                    if (packet.getLength() != 4) {
                        LOG.warn("Got more than just an xid! Len = "
                                + packet.getLength());
                    } else {
                        responseBuffer.clear();
                        responseBuffer.getInt(); // Skip the xid
                        // 把自己的myid放入
                        responseBuffer.putLong(myid);
                        // 获取到自己当前的选票
                        Vote current = getCurrentVote();
                        // 根据自己当前的状态来填充响应
                        switch (getPeerState()) {
                        case LOOKING:
                            responseBuffer.putLong(current.getId());
                            responseBuffer.putLong(current.getZxid());
                            break;
                        case LEADING:
                            responseBuffer.putLong(myid);
                            try {
                                long proposed;
                                synchronized(leader) {
                                    proposed = leader.lastProposed;
                                }
                                responseBuffer.putLong(proposed);
                            } catch (NullPointerException npe) {
                                // This can happen in state transitions,
                                // just ignore the request
                            }
                            break;
                        case FOLLOWING:
                            responseBuffer.putLong(current.getId());
                            try {
                                responseBuffer.putLong(follower.getZxid());
                            } catch (NullPointerException npe) {
                                // This can happen in state transitions,
                                // just ignore the request
                            }
                            break;
                        case OBSERVING:
                            // 如果是observing  则没有响应
                            // Do nothing, Observers keep themselves to
                            // themselves.
                            break;
                        }
                        // 填充的数据发送出去
                        packet.setData(b);
                        udpSocket.send(packet);
                    }
                    packet.setLength(b.length);
                }
            } catch (RuntimeException e) {
                LOG.warn("Unexpected runtime exception in ResponderThread",e);
            } catch (IOException e) {
                LOG.warn("Unexpected IO exception in ResponderThread",e);
            } finally {
                LOG.warn("QuorumPeer responder thread exited");
            }
        }
    }
    // 初始状态为 LOOKING
    private ServerState state = ServerState.LOOKING;
    
    private boolean reconfigFlag = false; // indicates that a reconfig just committed

    public synchronized void setPeerState(ServerState newState){
        state=newState;
    }
    public synchronized void reconfigFlagSet(){
       reconfigFlag = true;
    }
    public synchronized void reconfigFlagClear(){
       reconfigFlag = false;
    }
    public synchronized boolean isReconfigStateChange(){
       return reconfigFlag;
    }
    public synchronized ServerState getPeerState(){
        return state;
    }

    DatagramSocket udpSocket;

    private final AtomicReference<AddressTuple> myAddrs = new AtomicReference<>();

    /**
     * Resolves hostname for a given server ID.
     *
     * This method resolves hostname for a given server ID in both quorumVerifer
     * and lastSeenQuorumVerifier. If the server ID matches the local server ID,
     * it also updates myAddrs.
     */
    public void recreateSocketAddresses(long id) {
        QuorumVerifier qv = getQuorumVerifier();
        if (qv != null) {
            QuorumServer qs = qv.getAllMembers().get(id);
            if (qs != null) {
                qs.recreateSocketAddresses();
                if (id == getId()) {
                    setAddrs(qs.addr, qs.electionAddr, qs.clientAddr);
                }
            }
        }
        qv = getLastSeenQuorumVerifier();
        if (qv != null) {
            QuorumServer qs = qv.getAllMembers().get(id);
            if (qs != null) {
                qs.recreateSocketAddresses();
            }
        }
    }

    private AddressTuple getAddrs(){
        AddressTuple addrs = myAddrs.get();
        if (addrs != null) {
            return addrs;
        }
        try {
            synchronized (QV_LOCK) {
                addrs = myAddrs.get();
                while (addrs == null) {
                    QV_LOCK.wait();
                    addrs = myAddrs.get();
                }
                return addrs;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public InetSocketAddress getQuorumAddress(){
        return getAddrs().quorumAddr;
    }
    
    public InetSocketAddress getElectionAddress(){
        return getAddrs().electionAddr;
    }

    public InetSocketAddress getClientAddress(){
        final AddressTuple addrs = myAddrs.get();
        return (addrs == null) ? null : addrs.clientAddr;
    }
    
    private void setAddrs(InetSocketAddress quorumAddr, InetSocketAddress electionAddr, InetSocketAddress clientAddr){
        synchronized (QV_LOCK) {
            myAddrs.set(new AddressTuple(quorumAddr, electionAddr, clientAddr));
            QV_LOCK.notifyAll();
        }
    }
    
    private int electionType;

    Election electionAlg;

    ServerCnxnFactory cnxnFactory;
    ServerCnxnFactory secureCnxnFactory;

    private FileTxnSnapLog logFactory = null;

    private final QuorumStats quorumStats;

    AdminServer adminServer;

    public static QuorumPeer testingQuorumPeer() throws SaslException {
        return new QuorumPeer();
    }

    public QuorumPeer() throws SaslException {
        super("QuorumPeer");    // 设置线程的名字
        quorumStats = new QuorumStats(this);
        jmxRemotePeerBean = new HashMap<Long, RemotePeerBean>();
        // 反射创建org.apache.zookeeper.server.admin.JettyAdminServer
        adminServer = AdminServerFactory.createAdminServer();
        x509Util = new QuorumX509Util();
        // ssl相关的初始化
        initialize();
    }

    /**
     * For backward compatibility purposes, we instantiate QuorumMaj by default.
     */

    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File dataDir,
            File dataLogDir, int electionType,
            long myid, int tickTime, int initLimit, int syncLimit,
            ServerCnxnFactory cnxnFactory) throws IOException {
        this(quorumPeers, dataDir, dataLogDir, electionType, myid, tickTime,
                initLimit, syncLimit, false, cnxnFactory,
                new QuorumMaj(quorumPeers));
    }

    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File dataDir,
            File dataLogDir, int electionType,
            long myid, int tickTime, int initLimit, int syncLimit,
            boolean quorumListenOnAllIPs,
            ServerCnxnFactory cnxnFactory,
            QuorumVerifier quorumConfig) throws IOException {
        this();
        this.cnxnFactory = cnxnFactory;
        this.electionType = electionType;
        this.myid = myid;
        this.tickTime = tickTime;
        this.initLimit = initLimit;
        this.syncLimit = syncLimit;
        this.quorumListenOnAllIPs = quorumListenOnAllIPs;
        this.logFactory = new FileTxnSnapLog(dataLogDir, dataDir);
        this.zkDb = new ZKDatabase(this.logFactory);
        if(quorumConfig == null) quorumConfig = new QuorumMaj(quorumPeers);
        setQuorumVerifier(quorumConfig, false);
        adminServer = AdminServerFactory.createAdminServer();
    }

    public void initialize() throws SaslException {
        // init quorum auth server & learner
        if (isQuorumSaslAuthEnabled()) {
            Set<String> authzHosts = new HashSet<String>();
            for (QuorumServer qs : getView().values()) {
                authzHosts.add(qs.hostname);
            }
            authServer = new SaslQuorumAuthServer(isQuorumServerSaslAuthRequired(),
                    quorumServerLoginContext, authzHosts);
            authLearner = new SaslQuorumAuthLearner(isQuorumLearnerSaslAuthRequired(),
                    quorumServicePrincipal, quorumLearnerLoginContext);
        } else {
            authServer = new NullQuorumAuthServer();
            authLearner = new NullQuorumAuthLearner();
        }
    }

    QuorumStats quorumStats() {
        return quorumStats;
    }

    @Override
    public synchronized void start() {
        if (!getView().containsKey(myid)) {
            throw new RuntimeException("My id " + myid + " not in the peer list");
         }
        // 加载磁盘数据
        loadDataBase();
        // selector 接收客户端请求 并进行处理
        // 一个selector 多个worker 和 netty 很像
        // ************************************服务端请求接收****************
        startServerCnxnFactory();
        try {
            adminServer.start();
        } catch (AdminServerException e) {
            LOG.warn("Problem starting AdminServer", e);
            System.out.println(e);
        }
        // 开始leader选举前的准备
        startLeaderElection();
        // 开始选举
        // 注意 QuorumPeer 也就是本class 也是一个线程
        // 也就是说,此处的start会运行本类的run方法
        super.start();
    }

    private void loadDataBase() {
        try {
            zkDb.loadDataBase();

            // load the epochs
            long lastProcessedZxid = zkDb.getDataTree().lastProcessedZxid;
            long epochOfZxid = ZxidUtils.getEpochFromZxid(lastProcessedZxid);
            try {
                // 从文件中读取
                currentEpoch = readLongFromFile(CURRENT_EPOCH_FILENAME);
            } catch(FileNotFoundException e) {
            	// pick a reasonable epoch number
            	// this should only happen once when moving to a
            	// new code version
            	currentEpoch = epochOfZxid;
            	LOG.info(CURRENT_EPOCH_FILENAME
            	        + " not found! Creating with a reasonable default of {}. This should only happen when you are upgrading your installation",
            	        currentEpoch);
            	writeLongToFile(CURRENT_EPOCH_FILENAME, currentEpoch);
            }
            if (epochOfZxid > currentEpoch) {
                throw new IOException("The current epoch, " + ZxidUtils.zxidToString(currentEpoch) + ", is older than the last zxid, " + lastProcessedZxid);
            }
            try {
                // 已经接受的 epoch
                acceptedEpoch = readLongFromFile(ACCEPTED_EPOCH_FILENAME);
            } catch(FileNotFoundException e) {
            	// pick a reasonable epoch number
            	// this should only happen once when moving to a
            	// new code version
            	acceptedEpoch = epochOfZxid;
            	LOG.info(ACCEPTED_EPOCH_FILENAME
            	        + " not found! Creating with a reasonable default of {}. This should only happen when you are upgrading your installation",
            	        acceptedEpoch);
            	writeLongToFile(ACCEPTED_EPOCH_FILENAME, acceptedEpoch);
            }
            if (acceptedEpoch < currentEpoch) {
                throw new IOException("The accepted epoch, " + ZxidUtils.zxidToString(acceptedEpoch) + " is less than the current epoch, " + ZxidUtils.zxidToString(currentEpoch));
            }
        } catch(IOException ie) {
            LOG.error("Unable to load database on disk", ie);
            throw new RuntimeException("Unable to run quorum server ", ie);
        }
    }

    ResponderThread responder;

    synchronized public void stopLeaderElection() {
        responder.running = false;
        responder.interrupt();
    }
    synchronized public void startLeaderElection() {
       try {
           if (getPeerState() == ServerState.LOOKING) {
               // 创建选票
               // 由此可见,第一次选举投票,选举的是自己
               currentVote = new Vote(myid, getLastLoggedZxid(), getCurrentEpoch());
           }
       } catch(IOException e) {
           RuntimeException re = new RuntimeException(e.getMessage());
           re.setStackTrace(e.getStackTrace());
           throw re;
       }

       // if (!getView().containsKey(myid)) {
      //      throw new RuntimeException("My id " + myid + " not in the peer list");
        //}
        if (electionType == 0) {
            try {
                // 创建udpsocket
                udpSocket = new DatagramSocket(getQuorumAddress().getPort());
                // 响应线程
                responder = new ResponderThread();
                responder.start();
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
        }
        // 创建选举算法
        this.electionAlg = createElectionAlgorithm(electionType);
    }

    /**
     * Count the number of nodes in the map that could be followers.
     * @param peers
     * @return The number of followers in the map
     */
    protected static int countParticipants(Map<Long,QuorumServer> peers) {
      int count = 0;
      for (QuorumServer q : peers.values()) {
          if (q.type == LearnerType.PARTICIPANT) {
              count++;
          }
      }
      return count;
    }

    /**

     * This constructor is only used by the existing unit test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public QuorumPeer(Map<Long,QuorumServer> quorumPeers, File snapDir,
            File logDir, int clientPort, int electionAlg,
            long myid, int tickTime, int initLimit, int syncLimit)
        throws IOException
    {
        this(quorumPeers, snapDir, logDir, electionAlg, myid, tickTime, initLimit, syncLimit, false,
                ServerCnxnFactory.createFactory(getClientAddress(quorumPeers, myid, clientPort), -1),
                new QuorumMaj(quorumPeers));
    }

    /**
     * This constructor is only used by the existing unit test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public QuorumPeer(Map<Long,QuorumServer> quorumPeers, File snapDir,
            File logDir, int clientPort, int electionAlg,
            long myid, int tickTime, int initLimit, int syncLimit,
            QuorumVerifier quorumConfig)
        throws IOException
    {
        this(quorumPeers, snapDir, logDir, electionAlg,
                myid,tickTime, initLimit,syncLimit, false,
                ServerCnxnFactory.createFactory(getClientAddress(quorumPeers, myid, clientPort), -1),
                quorumConfig);
    }

    private static InetSocketAddress getClientAddress(Map<Long, QuorumServer> quorumPeers, long myid, int clientPort)
            throws IOException {
        QuorumServer quorumServer = quorumPeers.get(myid);
        if (null == quorumServer) {
            throw new IOException("No QuorumServer correspoding to myid " + myid);
        }
        if (null == quorumServer.clientAddr) {
            return new InetSocketAddress(clientPort);
        }
        if (quorumServer.clientAddr.getPort() != clientPort) {
            throw new IOException("QuorumServer port " + quorumServer.clientAddr.getPort()
                    + " does not match with given port " + clientPort);
        }
        return quorumServer.clientAddr;
    }

    /**
     * returns the highest zxid that this host has seen
     *
     * @return the highest zxid for this host
     */
    public long getLastLoggedZxid() {
        if (!zkDb.isInitialized()) {
            loadDataBase();
        }
        return zkDb.getDataTreeLastProcessedZxid();
    }

    public Follower follower;
    public Leader leader;
    public Observer observer;

    protected Follower makeFollower(FileTxnSnapLog logFactory) throws IOException {
        return new Follower(this, new FollowerZooKeeperServer(logFactory, this, this.zkDb));
    }

    protected Leader makeLeader(FileTxnSnapLog logFactory) throws IOException, X509Exception {
        return new Leader(this, new LeaderZooKeeperServer(logFactory, this, this.zkDb));
    }

    protected Observer makeObserver(FileTxnSnapLog logFactory) throws IOException {
        return new Observer(this, new ObserverZooKeeperServer(logFactory, this, this.zkDb));
    }

    @SuppressWarnings("deprecation")
    protected Election createElectionAlgorithm(int electionAlgorithm){
        Election le=null;

        //TODO: use a factory rather than a switch
        switch (electionAlgorithm) {
        case 0:
            le = new LeaderElection(this);
            break;
        case 1:
            le = new AuthFastLeaderElection(this);
            break;
        case 2:
            le = new AuthFastLeaderElection(this, true);
            break;
        case 3:
            QuorumCnxManager qcm = createCnxnManager();
            QuorumCnxManager oldQcm = qcmRef.getAndSet(qcm);
            if (oldQcm != null) {
                LOG.warn("Clobbering already-set QuorumCnxManager (restarting leader election?)");
                oldQcm.halt();
            }
            // 启动listener 处理连接请求
            // 此listener是处理选票 以及 通知等信息, 也就是信息都是server之间的
            // 但是当myid比较小 去 连接myid比较大的server时, 此listener会进行判断,小于自己的myid的连接,会关闭
            // 如果连接的myid 不小于自己,那么就根据 接收的socket创建 接收和发送线程 来进行交互
            QuorumCnxManager.Listener listener = qcm.listener;
            if(listener != null){
                listener.start();
                // 快速leader选举
                FastLeaderElection fle = new FastLeaderElection(this, qcm);
                // leader选举开始
                fle.start();
                // 记录选举算法
                le = fle;
            } else {
                LOG.error("Null listener when initializing cnx manager");
            }
            break;
        default:
            assert false;
        }
        return le;
    }

    @SuppressWarnings("deprecation")
    protected Election makeLEStrategy(){
        LOG.debug("Initializing leader election protocol...");
        if (getElectionType() == 0) {
            electionAlg = new LeaderElection(this);
        }
        // 获取选举算法
        return electionAlg;
    }

    synchronized protected void setLeader(Leader newLeader){
        leader=newLeader;
    }

    synchronized protected void setFollower(Follower newFollower){
        follower=newFollower;
    }
    // 设置自己角色 为 Observer
    synchronized protected void setObserver(Observer newObserver){
        observer=newObserver;
    }

    synchronized public ZooKeeperServer getActiveServer(){
        if(leader!=null)
            return leader.zk;
        else if(follower!=null)
            return follower.zk;
        else if (observer != null)
            return observer.zk;
        return null;
    }

    boolean shuttingDownLE = false;
    
    @Override
    public void run() {
        // 重新设置一下线程的名字
        updateThreadName();

        LOG.debug("Starting quorum peer");
        try {
            jmxQuorumBean = new QuorumBean(this);
            MBeanRegistry.getInstance().register(jmxQuorumBean, null);
            for(QuorumServer s: getView().values()){
                ZKMBeanInfo p;
                if (getId() == s.id) {
                    p = jmxLocalPeerBean = new LocalPeerBean(this);
                    try {
                        MBeanRegistry.getInstance().register(p, jmxQuorumBean);
                    } catch (Exception e) {
                        LOG.warn("Failed to register with JMX", e);
                        jmxLocalPeerBean = null;
                    }
                } else {
                    RemotePeerBean rBean = new RemotePeerBean(this, s);
                    try {
                        MBeanRegistry.getInstance().register(rBean, jmxQuorumBean);
                        jmxRemotePeerBean.put(s.id, rBean);
                    } catch (Exception e) {
                        LOG.warn("Failed to register with JMX", e);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxQuorumBean = null;
        }

        try {
            /*
             * Main loop
             * todo 重要  选举的操作 以及其他平时的数据交互操作
             */
            while (running) {
                // 根据自己当前的状态,来进行不同的操作
                switch (getPeerState()) {
                case LOOKING:
                    LOG.info("LOOKING");
                    // 是否是可读的模式
                    if (Boolean.getBoolean("readonlymode.enabled")) {
                        LOG.info("Attempting to start ReadOnlyZooKeeperServer");

                        // Create read-only server but don't start it immediately
                        final ReadOnlyZooKeeperServer roZk =
                            new ReadOnlyZooKeeperServer(logFactory, this, this.zkDb);
    
                        // Instead of starting roZk immediately, wait some grace
                        // period before we decide we're partitioned.
                        //
                        // Thread is used here because otherwise it would require
                        // changes in each of election strategy classes which is
                        // unnecessary code coupling.
                        // 只读模式的启动
                        Thread roZkMgr = new Thread() {
                            public void run() {
                                try {
                                    // lower-bound grace period to 2 secs
                                    sleep(Math.max(2000, tickTime));
                                    if (ServerState.LOOKING.equals(getPeerState())) {
                                        roZk.startup();
                                    }
                                } catch (InterruptedException e) {
                                    LOG.info("Interrupted while attempting to start ReadOnlyZooKeeperServer, not started");
                                } catch (Exception e) {
                                    LOG.error("FAILED to start ReadOnlyZooKeeperServer", e);
                                }
                            }
                        };
                        try {
                            roZkMgr.start();
                            reconfigFlagClear();
                            if (shuttingDownLE) {
                                shuttingDownLE = false;
                                startLeaderElection();
                            }
                            setCurrentVote(makeLEStrategy().lookForLeader());
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                            setPeerState(ServerState.LOOKING);
                        } finally {
                            // If the thread is in the the grace period, interrupt
                            // to come out of waiting.
                            roZkMgr.interrupt();
                            roZk.shutdown();
                        }
                    } else {
                        try {
                           reconfigFlagClear();
                            if (shuttingDownLE) {
                               shuttingDownLE = false;
                               startLeaderElection();
                               }
                            // leader选举
                            // 设置选票为 leader选票
                            // todo 重要  此处是真正的选举操作
                            setCurrentVote(makeLEStrategy().lookForLeader());
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                            setPeerState(ServerState.LOOKING);
                        }                        
                    }
                    break;
                case OBSERVING:
                    try {
                        LOG.info("OBSERVING");
                        // makeObserver 创建Observer
                        // setObserver 记录创建的Observer
                        setObserver(makeObserver(logFactory));
                        // 找到leader 并 注册
                        observer.observeLeader();
                    } catch (Exception e) {
                        LOG.warn("Unexpected exception",e );
                    } finally {
                        observer.shutdown();
                        setObserver(null);  
                       updateServerState();
                    }
                    break;
                case FOLLOWING:
                    try {
                       LOG.info("FOLLOWING");
                        setFollower(makeFollower(logFactory));
                        follower.followLeader();
                    } catch (Exception e) {
                       LOG.warn("Unexpected exception",e);
                    } finally {
                       follower.shutdown();
                       setFollower(null);
                       updateServerState();
                    }
                    break;
                case LEADING:
                    LOG.info("LEADING");
                    try {
                        setLeader(makeLeader(logFactory));
                        leader.lead();
                        setLeader(null);
                    } catch (Exception e) {
                        LOG.warn("Unexpected exception",e);
                    } finally {
                        if (leader != null) {
                            leader.shutdown("Forcing shutdown");
                            setLeader(null);
                        }
                        updateServerState();
                    }
                    break;
                }
                start_fle = Time.currentElapsedTime();
            }
        } finally {
            LOG.warn("QuorumPeer main thread exited");
            MBeanRegistry instance = MBeanRegistry.getInstance();
            instance.unregister(jmxQuorumBean);
            instance.unregister(jmxLocalPeerBean);

            for (RemotePeerBean remotePeerBean : jmxRemotePeerBean.values()) {
                instance.unregister(remotePeerBean);
            }

            jmxQuorumBean = null;
            jmxLocalPeerBean = null;
            jmxRemotePeerBean = null;
        }
    }

    private synchronized void updateServerState(){
       if (!reconfigFlag) {
           setPeerState(ServerState.LOOKING);
           LOG.warn("PeerState set to LOOKING");
           return;
       }
       
       if (getId() == getCurrentVote().getId()) {
           setPeerState(ServerState.LEADING);
           LOG.debug("PeerState set to LEADING");
       } else if (getLearnerType() == LearnerType.PARTICIPANT) {
           setPeerState(ServerState.FOLLOWING);
           LOG.debug("PeerState set to FOLLOWING");
       } else if (getLearnerType() == LearnerType.OBSERVER) {
           setPeerState(ServerState.OBSERVING);
           LOG.debug("PeerState set to OBSERVER");
       } else { // currently shouldn't happen since there are only 2 learner types
           setPeerState(ServerState.LOOKING);
           LOG.debug("Shouldn't be here");
       }       
       reconfigFlag = false;   
    }
    
    public void shutdown() {
        running = false;
        x509Util.close();
        if (leader != null) {
            leader.shutdown("quorum Peer shutdown");
        }
        if (follower != null) {
            follower.shutdown();
        }
        shutdownServerCnxnFactory();
        if(udpSocket != null) {
            udpSocket.close();
        }

        try {
            adminServer.shutdown();
        } catch (AdminServerException e) {
            LOG.warn("Problem stopping AdminServer", e);
        }

        if(getElectionAlg() != null){
            this.interrupt();
            getElectionAlg().shutdown();
        }
        try {
            zkDb.close();
        } catch (IOException ie) {
            LOG.warn("Error closing logs ", ie);
        }
    }

    /**
     * A 'view' is a node's current opinion of the membership of the entire
     * ensemble.
     */
    public Map<Long,QuorumPeer.QuorumServer> getView() {
        return Collections.unmodifiableMap(getQuorumVerifier().getAllMembers());
    }

    /**
     * Observers are not contained in this view, only nodes with
     * PeerType=PARTICIPANT.
     */
    public Map<Long,QuorumPeer.QuorumServer> getVotingView() {
        return getQuorumVerifier().getVotingMembers();
    }

    /**
     * Returns only observers, no followers.
     */
    public Map<Long,QuorumPeer.QuorumServer> getObservingView() {
       return getQuorumVerifier().getObservingMembers();
    }

    public synchronized Set<Long> getCurrentAndNextConfigVoters() {
        Set<Long> voterIds = new HashSet<Long>(getQuorumVerifier()
                .getVotingMembers().keySet());
        if (getLastSeenQuorumVerifier() != null) {
            voterIds.addAll(getLastSeenQuorumVerifier().getVotingMembers()
                    .keySet());
        }
        return voterIds;
    }
    
    /**
     * Check if a node is in the current view. With static membership, the
     * result of this check will never change; only when dynamic membership
     * is introduced will this be more useful.
     */
    public boolean viewContains(Long sid) {
        return this.getView().containsKey(sid);
    }

    /**
     * Only used by QuorumStats at the moment
     */
    public String[] getQuorumPeers() {
        List<String> l = new ArrayList<String>();
        synchronized (this) {
            if (leader != null) {
                for (LearnerHandler fh : leader.getLearners()) {
                    if (fh.getSocket() != null) {
                        String s = fh.getSocket().getRemoteSocketAddress().toString();
                        if (leader.isLearnerSynced(fh))
                            s += "*";
                        l.add(s);
                    }
                }
            } else if (follower != null) {
                l.add(follower.sock.getRemoteSocketAddress().toString());
            }
        }
        return l.toArray(new String[0]);
    }

    public String getServerState() {
        switch (getPeerState()) {
        case LOOKING:
            return QuorumStats.Provider.LOOKING_STATE;
        case LEADING:
            return QuorumStats.Provider.LEADING_STATE;
        case FOLLOWING:
            return QuorumStats.Provider.FOLLOWING_STATE;
        case OBSERVING:
            return QuorumStats.Provider.OBSERVING_STATE;
        }
        return QuorumStats.Provider.UNKNOWN_STATE;
    }


    /**
     * set the id of this quorum peer.
     */
    public void setMyid(long myid) {
        this.myid = myid;
    }

    /**
     * Get the number of milliseconds of each tick
     */
    public int getTickTime() {
        return tickTime;
    }

    /**
     * Set the number of milliseconds of each tick
     */
    public void setTickTime(int tickTime) {
        LOG.info("tickTime set to " + tickTime);
        this.tickTime = tickTime;
    }

    /** Maximum number of connections allowed from particular host (ip) */
    public int getMaxClientCnxnsPerHost() {
        if (cnxnFactory != null) {
            return cnxnFactory.getMaxClientCnxnsPerHost();
        }
        if (secureCnxnFactory != null) {
            return secureCnxnFactory.getMaxClientCnxnsPerHost();
        }
        return -1;
    }

    /** Whether local sessions are enabled */
    public boolean areLocalSessionsEnabled() {
        return localSessionsEnabled;
    }

    /** Whether to enable local sessions */
    public void enableLocalSessions(boolean flag) {
        LOG.info("Local sessions " + (flag ? "enabled" : "disabled"));
        localSessionsEnabled = flag;
    }

    /** Whether local sessions are allowed to upgrade to global sessions */
    public boolean isLocalSessionsUpgradingEnabled() {
        return localSessionsUpgradingEnabled;
    }

    /** Whether to allow local sessions to upgrade to global sessions */
    public void enableLocalSessionsUpgrading(boolean flag) {
        LOG.info("Local session upgrading " + (flag ? "enabled" : "disabled"));
        localSessionsUpgradingEnabled = flag;
    }

    /** minimum session timeout in milliseconds */
    public int getMinSessionTimeout() {
        return minSessionTimeout;
    }

    /** minimum session timeout in milliseconds */
    public void setMinSessionTimeout(int min) {
        LOG.info("minSessionTimeout set to " + min);
        this.minSessionTimeout = min;
    }

    /** maximum session timeout in milliseconds */
    public int getMaxSessionTimeout() {
        return maxSessionTimeout;
    }

    /** maximum session timeout in milliseconds */
    public void setMaxSessionTimeout(int max) {
        LOG.info("maxSessionTimeout set to " + max);
        this.maxSessionTimeout = max;
    }

    /**
     * Get the number of ticks that the initial synchronization phase can take
     */
    public int getInitLimit() {
        return initLimit;
    }

    /**
     * Set the number of ticks that the initial synchronization phase can take
     */
    public void setInitLimit(int initLimit) {
        LOG.info("initLimit set to " + initLimit);
        this.initLimit = initLimit;
    }

    /**
     * Get the current tick
     */
    public int getTick() {
        return tick.get();
    }

    public QuorumVerifier configFromString(String s) throws IOException, ConfigException{
        Properties props = new Properties();        
        props.load(new StringReader(s));
        return QuorumPeerConfig.parseDynamicConfig(props, electionType, false, false);
    }
    
    /**
     * Return QuorumVerifier object for the last committed configuration.
     */
    public QuorumVerifier getQuorumVerifier(){
        synchronized (QV_LOCK) {
            return quorumVerifier;
        }
    }

    /**
     * Return QuorumVerifier object for the last proposed configuration.
     */
    public QuorumVerifier getLastSeenQuorumVerifier(){
        synchronized (QV_LOCK) {
            return lastSeenQuorumVerifier;
        }
    }
    
    public synchronized void restartLeaderElection(QuorumVerifier qvOLD, QuorumVerifier qvNEW){
        if (qvOLD == null || !qvOLD.equals(qvNEW)) {
            LOG.warn("Restarting Leader Election");
            getElectionAlg().shutdown();
            shuttingDownLE = false;
            startLeaderElection();
        }           
    }

    public String getNextDynamicConfigFilename() {
        if (configFilename == null) {
            LOG.warn("configFilename is null! This should only happen in tests.");
            return null;
        }
        return configFilename + QuorumPeerConfig.nextDynamicConfigFileSuffix;
    }
    
    // On entry to this method, qcm must be non-null and the locks on both qcm and QV_LOCK
    // must be held.  We don't want quorumVerifier/lastSeenQuorumVerifier to change out from
    // under us, so we have to hold QV_LOCK; and since the call to qcm.connectOne() will take
    // the lock on qcm (and take QV_LOCK again inside that), the caller needs to have taken
    // qcm outside QV_LOCK to avoid a deadlock against other callers of qcm.connectOne().
    private void connectNewPeers(QuorumCnxManager qcm){
        if (quorumVerifier != null && lastSeenQuorumVerifier != null) {
            Map<Long, QuorumServer> committedView = quorumVerifier.getAllMembers();
            for (Entry<Long, QuorumServer> e : lastSeenQuorumVerifier.getAllMembers().entrySet()) {
                if (e.getKey() != getId() && !committedView.containsKey(e.getKey()))
                    qcm.connectOne(e.getKey());
            }
        }
    }

    public void setLastSeenQuorumVerifier(QuorumVerifier qv, boolean writeToDisk){
        // If qcm is non-null, we may call qcm.connectOne(), which will take the lock on qcm
        // and then take QV_LOCK.  Take the locks in the same order to ensure that we don't
        // deadlock against other callers of connectOne().  If qcmRef gets set in another
        // thread while we're inside the synchronized block, that does no harm; if we didn't
        // take a lock on qcm (because it was null when we sampled it), we won't call
        // connectOne() on it.  (Use of an AtomicReference is enough to guarantee visibility
        // of updates that provably happen in another thread before entering this method.)
        QuorumCnxManager qcm = qcmRef.get();
        Object outerLockObject = (qcm != null) ? qcm : QV_LOCK;
        synchronized (outerLockObject) {
            synchronized (QV_LOCK) {
                if (lastSeenQuorumVerifier != null && lastSeenQuorumVerifier.getVersion() > qv.getVersion()) {
                    LOG.error("setLastSeenQuorumVerifier called with stale config " + qv.getVersion() +
                            ". Current version: " + quorumVerifier.getVersion());
                }
                // assuming that a version uniquely identifies a configuration, so if
                // version is the same, nothing to do here.
                if (lastSeenQuorumVerifier != null &&
                        lastSeenQuorumVerifier.getVersion() == qv.getVersion()) {
                    return;
                }
                lastSeenQuorumVerifier = qv;
                if (qcm != null) {
                    connectNewPeers(qcm);
                }

                if (writeToDisk) {
                    try {
                        String fileName = getNextDynamicConfigFilename();
                        if (fileName != null) {
                            QuorumPeerConfig.writeDynamicConfig(fileName, qv, true);
                        }
                    } catch (IOException e) {
                        LOG.error("Error writing next dynamic config file to disk: ", e.getMessage());
                    }
                }
            }
        }
    }
    
    public QuorumVerifier setQuorumVerifier(QuorumVerifier qv, boolean writeToDisk){
        synchronized (QV_LOCK) {
            // 如果上次版本号大,则不更新
            if ((quorumVerifier != null) && (quorumVerifier.getVersion() >= qv.getVersion())) {
                // this is normal. For example - server found out about new config through FastLeaderElection gossiping
                // and then got the same config in UPTODATE message so its already known
                LOG.debug(getId() + " setQuorumVerifier called with known or old config " + qv.getVersion() +
                        ". Current version: " + quorumVerifier.getVersion());
                return quorumVerifier;
            }
            QuorumVerifier prevQV = quorumVerifier;
            quorumVerifier = qv;
            // 上次为null,或者 此次的version比上次大  则进行更新
            if (lastSeenQuorumVerifier == null || (qv.getVersion() > lastSeenQuorumVerifier.getVersion()))
                lastSeenQuorumVerifier = qv;
            // 是否更新到disk
            if (writeToDisk) {
                // some tests initialize QuorumPeer without a static config file
                if (configFilename != null) {
                    try {
                        String dynamicConfigFilename = makeDynamicConfigFilename(
                                qv.getVersion());
                        QuorumPeerConfig.writeDynamicConfig(
                                dynamicConfigFilename, qv, false);
                        QuorumPeerConfig.editStaticConfig(configFilename,
                                dynamicConfigFilename,
                                needEraseClientInfoFromStaticConfig());
                    } catch (IOException e) {
                        LOG.error("Error closing file: ", e.getMessage());
                    }
                } else {
                    LOG.info("writeToDisk == true but configFilename == null");
                }
            }

            if (qv.getVersion() == lastSeenQuorumVerifier.getVersion()) {
                QuorumPeerConfig.deleteFile(getNextDynamicConfigFilename());
            }
            QuorumServer qs = qv.getAllMembers().get(getId());
            if (qs != null) {
                // 记录自己的地址
                setAddrs(qs.addr, qs.electionAddr, qs.clientAddr);
            }
            return prevQV;
        }
    }

    private String makeDynamicConfigFilename(long version) {
        return configFilename + ".dynamic." + Long.toHexString(version);
    }

    private boolean needEraseClientInfoFromStaticConfig() {
        QuorumServer server = quorumVerifier.getAllMembers().get(getId());
        return (server != null && server.clientAddr != null);
    }

    /**
     * Get an instance of LeaderElection
     */
    public Election getElectionAlg(){
        return electionAlg;
    }

    /**
     * Get the synclimit
     */
    public int getSyncLimit() {
        return syncLimit;
    }

    /**
     * Set the synclimit
     */
    public void setSyncLimit(int syncLimit) {
        this.syncLimit = syncLimit;
    }
    
    
    /**
     * The syncEnabled can also be set via a system property.
     */
    public static final String SYNC_ENABLED = "zookeeper.observer.syncEnabled";
    
    /**
     * Return syncEnabled.
     * 
     * @return
     */
    public boolean getSyncEnabled() {
        if (System.getProperty(SYNC_ENABLED) != null) {
            LOG.info(SYNC_ENABLED + "=" + Boolean.getBoolean(SYNC_ENABLED));   
            return Boolean.getBoolean(SYNC_ENABLED);
        } else {        
            return syncEnabled;
        }
    }
    
    /**
     * Set syncEnabled.
     * 
     * @param syncEnabled
     */
    public void setSyncEnabled(boolean syncEnabled) {
        this.syncEnabled = syncEnabled;
    }

    /**
     * Gets the election type
     */
    public int getElectionType() {
        return electionType;
    }

    /**
     * Sets the election type
     */
    public void setElectionType(int electionType) {
        this.electionType = electionType;
    }

    public boolean getQuorumListenOnAllIPs() {
        return quorumListenOnAllIPs;
    }

    public void setQuorumListenOnAllIPs(boolean quorumListenOnAllIPs) {
        this.quorumListenOnAllIPs = quorumListenOnAllIPs;
    }

    public void setCnxnFactory(ServerCnxnFactory cnxnFactory) {
        this.cnxnFactory = cnxnFactory;
    }

    public void setSecureCnxnFactory(ServerCnxnFactory secureCnxnFactory) {
        this.secureCnxnFactory = secureCnxnFactory;
    }

    public void setSslQuorum(boolean sslQuorum) {
        if (sslQuorum) {
            LOG.info("Using TLS encrypted quorum communication");
        } else {
            LOG.info("Using insecure (non-TLS) quorum communication");
        }
        this.sslQuorum = sslQuorum;
    }

    public void setUsePortUnification(boolean shouldUsePortUnification) {
        LOG.info("Port unification {}", shouldUsePortUnification ? "enabled" : "disabled");
        this.shouldUsePortUnification = shouldUsePortUnification;
    }

    private void startServerCnxnFactory() {
        // 此主要是 selector 处理接收,也就是客户端的连接请求处理
        if (cnxnFactory != null) {
            cnxnFactory.start();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.start();
        }
    }

    private void shutdownServerCnxnFactory() {
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.shutdown();
        }
    }

    // Leader and learner will control the zookeeper server and pass it into QuorumPeer.
    public void setZooKeeperServer(ZooKeeperServer zks) {
        if (cnxnFactory != null) {
            cnxnFactory.setZooKeeperServer(zks);
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.setZooKeeperServer(zks);
        }
    }

    public void closeAllConnections() {
        if (cnxnFactory != null) {
            cnxnFactory.closeAll();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.closeAll();
        }
    }

    public int getClientPort() {
        if (cnxnFactory != null) {
            return cnxnFactory.getLocalPort();
        }
        return -1;
    }

    public void setTxnFactory(FileTxnSnapLog factory) {
        this.logFactory = factory;
    }

    public FileTxnSnapLog getTxnFactory() {
        return this.logFactory;
    }

    /**
     * set zk database for this node
     * @param database
     */
    public void setZKDatabase(ZKDatabase database) {
        this.zkDb = database;
    }

    protected ZKDatabase getZkDb() {
        return zkDb;
    }

    public synchronized void initConfigInZKDatabase() {   
        if (zkDb != null) zkDb.initConfigInZKDatabase(getQuorumVerifier());
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * get reference to QuorumCnxManager
     */
    public QuorumCnxManager getQuorumCnxManager() {
        return qcmRef.get();
    }
    private long readLongFromFile(String name) throws IOException {
        File file = new File(logFactory.getSnapDir(), name);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = "";
        try {
            line = br.readLine();
            return Long.parseLong(line);
        } catch(NumberFormatException e) {
            throw new IOException("Found " + line + " in " + file);
        } finally {
            br.close();
        }
    }

    private long acceptedEpoch = -1;
    private long currentEpoch = -1;

    public static final String CURRENT_EPOCH_FILENAME = "currentEpoch";

    public static final String ACCEPTED_EPOCH_FILENAME = "acceptedEpoch";

	/**
	 * Write a long value to disk atomically. Either succeeds or an exception
	 * is thrown.
	 * @param name file name to write the long to
	 * @param value the long value to write to the named file
	 * @throws IOException if the file cannot be written atomically
	 */
    private void writeLongToFile(String name, final long value) throws IOException {
        File file = new File(logFactory.getSnapDir(), name);
        new AtomicFileWritingIdiom(file, new WriterStatement() {
            @Override
            public void write(Writer bw) throws IOException {
                bw.write(Long.toString(value));
            }
        });
    }

    public long getCurrentEpoch() throws IOException {
        if (currentEpoch == -1) {
            currentEpoch = readLongFromFile(CURRENT_EPOCH_FILENAME);
        }
        return currentEpoch;
    }

    public long getAcceptedEpoch() throws IOException {
        if (acceptedEpoch == -1) {
            acceptedEpoch = readLongFromFile(ACCEPTED_EPOCH_FILENAME);
        }
        return acceptedEpoch;
    }

    public void setCurrentEpoch(long e) throws IOException {
        currentEpoch = e;
        writeLongToFile(CURRENT_EPOCH_FILENAME, e);

    }

    public void setAcceptedEpoch(long e) throws IOException {
        acceptedEpoch = e;
        writeLongToFile(ACCEPTED_EPOCH_FILENAME, e);
    }
   
    public boolean processReconfig(QuorumVerifier qv, Long suggestedLeaderId, Long zxid, boolean restartLE) {
       if (!QuorumPeerConfig.isReconfigEnabled()) {
           LOG.debug("Reconfig feature is disabled, skip reconfig processing.");
           return false;
       }

       InetSocketAddress oldClientAddr = getClientAddress();

       // update last committed quorum verifier, write the new config to disk
       // and restart leader election if config changed.
       // 更新last committed quorum verifier
       QuorumVerifier prevQV = setQuorumVerifier(qv, true);

       // There is no log record for the initial config, thus after syncing
       // with leader
       // /zookeeper/config is empty! it is also possible that last committed
       // config is propagated during leader election
       // without the propagation the corresponding log records.
       // so we should explicitly do this (this is not necessary when we're
       // already a Follower/Observer, only
       // for Learner):
       initConfigInZKDatabase();

       if (prevQV.getVersion() < qv.getVersion() && !prevQV.equals(qv)) {
           Map<Long, QuorumServer> newMembers = qv.getAllMembers();
           updateRemotePeerMXBeans(newMembers);
           // 重启leader选举
           if (restartLE) restartLeaderElection(prevQV, qv);

           QuorumServer myNewQS = newMembers.get(getId());
           if (myNewQS != null && myNewQS.clientAddr != null
                   && !myNewQS.clientAddr.equals(oldClientAddr)) {
               cnxnFactory.reconfigure(myNewQS.clientAddr);
               updateThreadName();
           }

           boolean roleChange = updateLearnerType(qv);
           boolean leaderChange = false;
           if (suggestedLeaderId != null) {
               // zxid should be non-null too
               leaderChange = updateVote(suggestedLeaderId, zxid);
           } else {
               long currentLeaderId = getCurrentVote().getId();
               QuorumServer myleaderInCurQV = prevQV.getVotingMembers().get(currentLeaderId);
               QuorumServer myleaderInNewQV = qv.getVotingMembers().get(currentLeaderId);
               leaderChange = (myleaderInCurQV == null || myleaderInCurQV.addr == null || 
                               myleaderInNewQV == null || !myleaderInCurQV.addr.equals(myleaderInNewQV.addr));
               // we don't have a designated leader - need to go into leader
               // election
               reconfigFlagClear();
           }
           
           if (roleChange || leaderChange) {
               return true;
           }
       }
       return false;

   }

    private void updateRemotePeerMXBeans(Map<Long, QuorumServer> newMembers) {
        Set<Long> existingMembers = new HashSet<Long>(newMembers.keySet());
        existingMembers.retainAll(jmxRemotePeerBean.keySet());
        for (Long id : existingMembers) {
            RemotePeerBean rBean = jmxRemotePeerBean.get(id);
            rBean.setQuorumServer(newMembers.get(id));
        }

        Set<Long> joiningMembers = new HashSet<Long>(newMembers.keySet());
        joiningMembers.removeAll(jmxRemotePeerBean.keySet());
        joiningMembers.remove(getId()); // remove self as it is local bean
        for (Long id : joiningMembers) {
            QuorumServer qs = newMembers.get(id);
            RemotePeerBean rBean = new RemotePeerBean(this, qs);
            try {
                MBeanRegistry.getInstance().register(rBean, jmxQuorumBean);
                jmxRemotePeerBean.put(qs.id, rBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
            }
        }

        Set<Long> leavingMembers = new HashSet<Long>(jmxRemotePeerBean.keySet());
        leavingMembers.removeAll(newMembers.keySet());
        for (Long id : leavingMembers) {
            RemotePeerBean rBean = jmxRemotePeerBean.remove(id);
            try {
                MBeanRegistry.getInstance().unregister(rBean);
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
        }
    }

   private boolean updateLearnerType(QuorumVerifier newQV) {        
       //check if I'm an observer in new config
       if (newQV.getObservingMembers().containsKey(getId())) {
           if (getLearnerType()!=LearnerType.OBSERVER){
               setLearnerType(LearnerType.OBSERVER);
               LOG.info("Becoming an observer");
               reconfigFlagSet();
               return true;
           } else {
               return false;           
           }
       } else if (newQV.getVotingMembers().containsKey(getId())) {
           if (getLearnerType()!=LearnerType.PARTICIPANT){
               setLearnerType(LearnerType.PARTICIPANT);
               LOG.info("Becoming a voting participant");
               reconfigFlagSet();
               return true;
           } else {
               return false;
           }
       }
       // I'm not in the view
      if (getLearnerType()!=LearnerType.PARTICIPANT){
          setLearnerType(LearnerType.PARTICIPANT);
          LOG.info("Becoming a non-voting participant");
          reconfigFlagSet();
          return true;
      }
      return false;
   }
   
   private boolean updateVote(long designatedLeader, long zxid){       
       Vote currentVote = getCurrentVote();
       if (currentVote!=null && designatedLeader != currentVote.getId()) {
           setCurrentVote(new Vote(designatedLeader, zxid));
           reconfigFlagSet();
           LOG.warn("Suggested leader: " + designatedLeader);
           return true;
       }
       return false;
   }
 
    /**
     * Updates leader election info to avoid inconsistencies when
     * a new server tries to join the ensemble.
     *
     * Here is the inconsistency scenario we try to solve by updating the peer 
     * epoch after following leader:
     *
     * Let's say we have an ensemble with 3 servers z1, z2 and z3.
     *
     * 1. z1, z2 were following z3 with peerEpoch to be 0xb8, the new epoch is 
     *    0xb9, aka current accepted epoch on disk.
     * 2. z2 get restarted, which will use 0xb9 as it's peer epoch when loading
     *    the current accept epoch from disk.
     * 3. z2 received notification from z1 and z3, which is following z3 with 
     *    epoch 0xb8, so it started following z3 again with peer epoch 0xb8.
     * 4. before z2 successfully connected to z3, z3 get restarted with new 
     *    epoch 0xb9.
     * 5. z2 will retry around a few round (default 5s) before giving up, 
     *    meanwhile it will report z3 as leader.
     * 6. z1 restarted, and looking with peer epoch 0xb9.
     * 7. z1 voted z3, and z3 was elected as leader again with peer epoch 0xb9.
     * 8. z2 successfully connected to z3 before giving up, but with peer 
     *    epoch 0xb8.
     * 9. z1 get restarted, looking for leader with peer epoch 0xba, but cannot 
     *    join, because z2 is reporting peer epoch 0xb8, while z3 is reporting 
     *    0xb9.
     *
     * By updating the election vote after actually following leader, we can 
     * avoid this kind of stuck happened.
     *
     * Btw, the zxid and electionEpoch could be inconsistent because of the same 
     * reason, it's better to update these as well after syncing with leader, but 
     * that required protocol change which is non trivial. This problem is worked 
     * around by skipping comparing the zxid and electionEpoch when counting for 
     * votes for out of election servers during looking for leader.
     * 
     * {@see https://issues.apache.org/jira/browse/ZOOKEEPER-1732}
     */
    protected void updateElectionVote(long newEpoch) {
        Vote currentVote = getCurrentVote();
        if (currentVote != null) {
            setCurrentVote(new Vote(currentVote.getId(),
                currentVote.getZxid(),
                currentVote.getElectionEpoch(),
                newEpoch,
                currentVote.getState()));
        }
    }

    private void updateThreadName() {
        String plain = cnxnFactory != null ?
                cnxnFactory.getLocalAddress() != null ?
                        cnxnFactory.getLocalAddress().toString() : "disabled" : "disabled";
        String secure = secureCnxnFactory != null ? secureCnxnFactory.getLocalAddress().toString() : "disabled";
        setName(String.format("QuorumPeer[myid=%d](plain=%s)(secure=%s)", getId(), plain, secure));
    }

    /**
     * Sets the time taken for leader election in milliseconds.
     *
     * @param electionTimeTaken time taken for leader election
     */
    void setElectionTimeTaken(long electionTimeTaken) {
        this.electionTimeTaken = electionTimeTaken;
    }

    /**
     * @return the time taken for leader election in milliseconds.
     */
    long getElectionTimeTaken() {
        return electionTimeTaken;
    }

    void setQuorumServerSaslRequired(boolean serverSaslRequired) {
        quorumServerSaslAuthRequired = serverSaslRequired;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED,
                serverSaslRequired);
    }

    void setQuorumLearnerSaslRequired(boolean learnerSaslRequired) {
        quorumLearnerSaslAuthRequired = learnerSaslRequired;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED,
                learnerSaslRequired);
    }

    void setQuorumSaslEnabled(boolean enableAuth) {
        quorumSaslEnableAuth = enableAuth;
        if (!quorumSaslEnableAuth) {
            LOG.info("QuorumPeer communication is not secured! (SASL auth disabled)");
        } else {
            LOG.info("{} set to {}",
                    QuorumAuth.QUORUM_SASL_AUTH_ENABLED, enableAuth);
        }
    }

    void setQuorumServicePrincipal(String servicePrincipal) {
        quorumServicePrincipal = servicePrincipal;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_KERBEROS_SERVICE_PRINCIPAL,
                quorumServicePrincipal);
    }

    void setQuorumLearnerLoginContext(String learnerContext) {
        quorumLearnerLoginContext = learnerContext;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_LEARNER_SASL_LOGIN_CONTEXT,
                quorumLearnerLoginContext);
    }

    void setQuorumServerLoginContext(String serverContext) {
        quorumServerLoginContext = serverContext;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT,
                quorumServerLoginContext);
    }

    void setQuorumCnxnThreadsSize(int qCnxnThreadsSize) {
        if (qCnxnThreadsSize > QUORUM_CNXN_THREADS_SIZE_DEFAULT_VALUE) {
            quorumCnxnThreadsSize = qCnxnThreadsSize;
        }
        LOG.info("quorum.cnxn.threads.size set to {}", quorumCnxnThreadsSize);
    }

    boolean isQuorumSaslAuthEnabled() {
        return quorumSaslEnableAuth;
    }

    private boolean isQuorumServerSaslAuthRequired() {
        return quorumServerSaslAuthRequired;
    }

    private boolean isQuorumLearnerSaslAuthRequired() {
        return quorumLearnerSaslAuthRequired;
    }

    public QuorumCnxManager createCnxnManager() {
        return new QuorumCnxManager(this,
                this.getId(),
                this.getView(), // 此view 记录了所有的server
                this.authServer,
                this.authLearner,
                this.tickTime * this.syncLimit, // socketTimeou的设置
                this.getQuorumListenOnAllIPs(),
                this.quorumCnxnThreadsSize,
                this.isQuorumSaslAuthEnabled());
    }

    boolean isLeader(long id) {
        Vote vote = getCurrentVote();
        return vote != null && id == vote.getId();
    }
}
