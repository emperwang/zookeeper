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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLSocket;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the superclass of two of the three main actors in a ZK
 * ensemble: Followers and Observers. Both Followers and Observers share 
 * a good deal of code which is moved into Peer to avoid duplication. 
 */
public class Learner {       
    static class PacketInFlight {
        TxnHeader hdr;
        Record rec;
    }
    QuorumPeer self;
    LearnerZooKeeperServer zk;
    
    protected BufferedOutputStream bufferedOutput;
    
    protected Socket sock;

    /**
     * Socket getter
     * @return 
     */
    public Socket getSocket() {
        return sock;
    }
    // 和leader进行数据同步时的 socket io流
    protected InputArchive leaderIs;
    protected OutputArchive leaderOs;  
    /** the protocol version of the leader */
    protected int leaderProtocolVersion = 0x01;
    
    protected static final Logger LOG = LoggerFactory.getLogger(Learner.class);

    static final private boolean nodelay = System.getProperty("follower.nodelay", "true").equals("true");
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }   
    
    final ConcurrentHashMap<Long, ServerCnxn> pendingRevalidations =
        new ConcurrentHashMap<Long, ServerCnxn>();
    
    public int getPendingRevalidationsCount() {
        return pendingRevalidations.size();
    }
    
    /**
     * validate a session for a client
     *
     * @param clientId
     *                the client to be revalidated
     * @param timeout
     *                the timeout for which the session is valid
     * @return
     * @throws IOException
     */
    void validateSession(ServerCnxn cnxn, long clientId, int timeout)
            throws IOException {
        LOG.info("Revalidating client: 0x" + Long.toHexString(clientId));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(clientId);
        dos.writeInt(timeout);
        dos.close();
        QuorumPacket qp = new QuorumPacket(Leader.REVALIDATE, -1, baos
                .toByteArray(), null);
        pendingRevalidations.put(clientId, cnxn);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                                     ZooTrace.SESSION_TRACE_MASK,
                                     "To validate session 0x"
                                     + Long.toHexString(clientId));
        }
        writePacket(qp, true);
    }     
    
    /**
     * write a packet to the leader
     *
     * @param pp
     *                the proposal packet to be sent to the leader
     * @throws IOException
     */
    void writePacket(QuorumPacket pp, boolean flush) throws IOException {
        synchronized (leaderOs) {
            if (pp != null) {
                // 调用流程
                // 1. BinaryOutputArchive.writeRecord
                // 2. --> r.serialize
                // 3. --> QuorumPacket.serialize
                // 4. --> leaderOs.out.{writeInt writeLong wirteBuffer}
                // leaderOs中的out是和 leader连接的输出流,通过此out进行写出,最终就通过 socket 写入到leader
                leaderOs.writeRecord(pp, "packet");
            }
            if (flush) {
                bufferedOutput.flush();
            }
        }
    }

    /**
     * read a packet from the leader
     *
     * @param pp
     *                the packet to be instantiated
     * @throws IOException
     */
    void readPacket(QuorumPacket pp) throws IOException {
        synchronized (leaderIs) {
            leaderIs.readRecord(pp, "packet");
        }
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        if (pp.getType() == Leader.PING) {
            traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logQuorumPacket(LOG, traceMask, 'i', pp);
        }
    }
    
    /**
     * send a request packet to the leader
     *
     * @param request
     *                the request from the client
     * @throws IOException
     */
    void request(Request request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream oa = new DataOutputStream(baos);
        oa.writeLong(request.sessionId);
        oa.writeInt(request.cxid);
        oa.writeInt(request.type);
        if (request.request != null) {
            request.request.rewind();
            int len = request.request.remaining();
            byte b[] = new byte[len];
            request.request.get(b);
            request.request.rewind();
            oa.write(b);
        }
        oa.close();
        QuorumPacket qp = new QuorumPacket(Leader.REQUEST, -1, baos
                .toByteArray(), request.authInfo);
        writePacket(qp, true);
    }
    
    /**
     * Returns the address of the node we think is the leader.
     */
    protected QuorumServer findLeader() {
        QuorumServer leaderServer = null;
        // Find the leader by id
        // 选举完成后, 当前选票记录了最终的选票结果,选票中有leader的具体信息
        Vote current = self.getCurrentVote();
        // 获取leader的地址
        for (QuorumServer s : self.getView().values()) {
            if (s.id == current.getId()) {
                // Ensure we have the leader's correct IP address before
                // attempting to connect.
                // 重新创建 leader的地址
                s.recreateSocketAddresses();
                leaderServer = s;
                break;
            }
        }
        if (leaderServer == null) {
            LOG.warn("Couldn't find the leader with id = "
                    + current.getId());
        }
        return leaderServer;
    }
   
    /**
     * Overridable helper method to return the System.nanoTime().
     * This method behaves identical to System.nanoTime().
     */
    protected long nanoTime() {
        return System.nanoTime();
    }

    /**
     * Overridable helper method to simply call sock.connect(). This can be
     * overriden in tests to fake connection success/failure for connectToLeader. 
     */
    protected void sockConnect(Socket sock, InetSocketAddress addr, int timeout) 
    throws IOException {
        sock.connect(addr, timeout);
    }

    /**
     * Establish a connection with the Leader found by findLeader. Retries
     * until either initLimit time has elapsed or 5 tries have happened. 
     * @param addr - the address of the Leader to connect to.
     * @throws IOException - if the socket connection fails on the 5th attempt
     * <li>if there is an authentication failure while connecting to leader</li>
     * @throws ConnectException
     * @throws InterruptedException
     */
    protected void connectToLeader(InetSocketAddress addr, String hostname)
            throws IOException, InterruptedException, X509Exception {
        this.sock = createSocket();

        int initLimitTime = self.tickTime * self.initLimit;
        int remainingInitLimitTime = initLimitTime;
        long startNanoTime = nanoTime();
        // 这里可以看到 连接leader时  如果失败会进行重试, 重试次数为5
        for (int tries = 0; tries < 5; tries++) {
            try {
                // recalculate the init limit time because retries sleep for 1000 milliseconds
                remainingInitLimitTime = initLimitTime - (int)((nanoTime() - startNanoTime) / 1000000);
                if (remainingInitLimitTime <= 0) {
                    LOG.error("initLimit exceeded on retries.");
                    throw new IOException("initLimit exceeded on retries.");
                }
                // 连接到 leader
                sockConnect(sock, addr, Math.min(self.tickTime * self.syncLimit, remainingInitLimitTime));
                if (self.isSslQuorum())  {
                    ((SSLSocket) sock).startHandshake();    // 如果是ssl加密的,那么先开始进行握手操作
                }
                sock.setTcpNoDelay(nodelay);
                break;
            } catch (IOException e) {
                remainingInitLimitTime = initLimitTime - (int)((nanoTime() - startNanoTime) / 1000000);

                if (remainingInitLimitTime <= 1000) {
                    LOG.error("Unexpected exception, initLimit exceeded. tries=" + tries +
                             ", remaining init limit=" + remainingInitLimitTime +
                             ", connecting to " + addr,e);
                    throw e;
                } else if (tries >= 4) {
                    LOG.error("Unexpected exception, retries exceeded. tries=" + tries +
                             ", remaining init limit=" + remainingInitLimitTime +
                             ", connecting to " + addr,e);
                    throw e;
                } else {
                    LOG.warn("Unexpected exception, tries=" + tries +
                            ", remaining init limit=" + remainingInitLimitTime +
                            ", connecting to " + addr,e);
                    this.sock = createSocket();
                }
            }
            Thread.sleep(1000);
        }

        self.authLearner.authenticate(sock, hostname);
        // 此leaderIs 是和 leader交互的数据流
        leaderIs = BinaryInputArchive.getArchive(new BufferedInputStream(
                sock.getInputStream()));
        // 输出流
        bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
        // 此leaderOs 是和 leader交互的数据输出流
        leaderOs = BinaryOutputArchive.getArchive(bufferedOutput);
    }

    private Socket createSocket() throws X509Exception, IOException {
        Socket sock;
        if (self.isSslQuorum()) {
            sock = self.getX509Util().createSSLSocket();
        } else {
            sock = new Socket();
        }
        sock.setSoTimeout(self.tickTime * self.initLimit);
        return sock;
    }

    /**
     * Once connected to the leader, perform the handshake protocol to
     * establish a following / observing connection. 
     * @param pktType
     * @return the zxid the Leader sends for synchronization purposes.
     * @throws IOException
     */
    protected long registerWithLeader(int pktType) throws IOException{
        /*
         * Send follower info, including last zxid and sid
         */
        // 上次接收到的Zxid
    	long lastLoggedZxid = self.getLastLoggedZxid();
    	// 数据包,记录要发送的数据
        QuorumPacket qp = new QuorumPacket();
        // 数据包的类型
        qp.setType(pktType);
        // 此次数据的 Zxid
        qp.setZxid(ZxidUtils.makeZxid(self.getAcceptedEpoch(), 0));
        
        /*
         * Add sid to payload
         */
        // LeaderInfo 包装了此次的 数据包的数据
        LearnerInfo li = new LearnerInfo(self.getId(), 0x10000, self.getQuorumVerifier().getVersion());
        // 存储要发送的数据
        ByteArrayOutputStream bsid = new ByteArrayOutputStream();
        // 静态方法 来创建写入流
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(bsid);
        // 把 LearnerInfo中的数据写入到 ByteArrayOutputStream
        boa.writeRecord(li, "LearnerInfo");
        // 保存数据
        qp.setData(bsid.toByteArray());
        // 把数据通过 leadOps 写入到leader
        writePacket(qp, true);
        // 接收leader传递回来的消息
        readPacket(qp);
        // 从leader传递回的package中的zxid 获取 epoch
        final long newEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());
		if (qp.getType() == Leader.LEADERINFO) {
        	// we are connected to a 1.0 server so accept the new epoch and read the next packet
        	// 获取leader发送的data
            leaderProtocolVersion = ByteBuffer.wrap(qp.getData()).getInt();
        	byte epochBytes[] = new byte[4];
        	final ByteBuffer wrappedEpochBytes = ByteBuffer.wrap(epochBytes);
        	// 如果leader的epoch大于自己的,则更新
        	if (newEpoch > self.getAcceptedEpoch()) {
        		wrappedEpochBytes.putInt((int)self.getCurrentEpoch());
        		// 记录从 leader接收的 Epoch
        		self.setAcceptedEpoch(newEpoch);
        		// 相等,则
        	} else if (newEpoch == self.getAcceptedEpoch()) {
        		// since we have already acked an epoch equal to the leaders, we cannot ack
        		// again, but we still need to send our lastZxid to the leader so that we can
        		// sync with it if it does assume leadership of the epoch.
        		// the -1 indicates that this reply should not count as an ack for the new epoch
                wrappedEpochBytes.putInt(-1);
                // 如果leader 比 自己的小, 则报错
        	} else {
        		throw new IOException("Leaders epoch, " + newEpoch + " is less than accepted epoch, " + self.getAcceptedEpoch());
        	}
        	// 创建响应的 packet , 回复给leader
        	QuorumPacket ackNewEpoch = new QuorumPacket(Leader.ACKEPOCH, lastLoggedZxid, epochBytes, null);
        	// 把响应的package 写回到 leader
        	writePacket(ackNewEpoch, true);
        	// 使用从leader接收到的Epoch 生成新的 Zxid 并返回
            return ZxidUtils.makeZxid(newEpoch, 0);
        } else {
		    // 新的Epoch大于自己的,则更新
        	if (newEpoch > self.getAcceptedEpoch()) {
        		self.setAcceptedEpoch(newEpoch);
        	}
        	// 如果不是 newleader,则报错
            if (qp.getType() != Leader.NEWLEADER) {
                LOG.error("First packet should have been NEWLEADER");
                throw new IOException("First packet should have been NEWLEADER");
            }
            // 返回获取到的zxid
            return qp.getZxid();
        }
    } 
    
    /**
     * Finally, synchronize our history with the Leader. 
     * @param newLeaderZxid
     * @throws IOException
     * @throws InterruptedException
     */
    protected void syncWithLeader(long newLeaderZxid) throws Exception{
        QuorumPacket ack = new QuorumPacket(Leader.ACK, 0, null, null);
        QuorumPacket qp = new QuorumPacket();
        long newEpoch = ZxidUtils.getEpochFromZxid(newLeaderZxid);
        
        QuorumVerifier newLeaderQV = null;
        
        // In the DIFF case we don't need to do a snapshot because the transactions will sync on top of any existing snapshot
        // For SNAP and TRUNC the snapshot is needed to save that history
        boolean snapshotNeeded = true;
        readPacket(qp);
        LinkedList<Long> packetsCommitted = new LinkedList<Long>();
        LinkedList<PacketInFlight> packetsNotCommitted = new LinkedList<PacketInFlight>();
        synchronized (zk) {
            if (qp.getType() == Leader.DIFF) {
                LOG.info("Getting a diff from the leader 0x{}", Long.toHexString(qp.getZxid()));
                snapshotNeeded = false;
            }
            else if (qp.getType() == Leader.SNAP) {
                LOG.info("Getting a snapshot from leader 0x" + Long.toHexString(qp.getZxid()));
                // The leader is going to dump the database
                // db is clear as part of deserializeSnapshot()
                zk.getZKDatabase().deserializeSnapshot(leaderIs);
                // ZOOKEEPER-2819: overwrite config node content extracted
                // from leader snapshot with local config, to avoid potential
                // inconsistency of config node content during rolling restart.
                if (!QuorumPeerConfig.isReconfigEnabled()) {
                    LOG.debug("Reset config node content from local config after deserialization of snapshot.");
                    zk.getZKDatabase().initConfigInZKDatabase(self.getQuorumVerifier());
                }
                String signature = leaderIs.readString("signature");
                if (!signature.equals("BenWasHere")) {
                    LOG.error("Missing signature. Got " + signature);
                    throw new IOException("Missing signature");                   
                }
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());
            } else if (qp.getType() == Leader.TRUNC) {
                //we need to truncate the log to the lastzxid of the leader
                LOG.warn("Truncating log to get in sync with the leader 0x"
                        + Long.toHexString(qp.getZxid()));
                boolean truncated=zk.getZKDatabase().truncateLog(qp.getZxid());
                if (!truncated) {
                    // not able to truncate the log
                    LOG.error("Not able to truncate the log "
                            + Long.toHexString(qp.getZxid()));
                    System.exit(13);
                }
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());

            }
            else {
                LOG.error("Got unexpected packet from leader: {}, exiting ... ",
                          LearnerHandler.packetToString(qp));
                System.exit(13);

            }
            zk.getZKDatabase().initConfigInZKDatabase(self.getQuorumVerifier());
            // 记录此 session
            zk.createSessionTracker();            
            
            long lastQueued = 0;

            // in Zab V1.0 (ZK 3.4+) we might take a snapshot when we get the NEWLEADER message, but in pre V1.0
            // we take the snapshot on the UPDATE message, since Zab V1.0 also gets the UPDATE (after the NEWLEADER)
            // we need to make sure that we don't take the snapshot twice.
            boolean isPreZAB1_0 = true;
            //If we are not going to take the snapshot be sure the transactions are not applied in memory
            // but written out to the transaction log
            boolean writeToTxnLog = !snapshotNeeded;
            // we are now going to start getting transactions to apply followed by an UPTODATE
            outerLoop:
            while (self.isRunning()) {
                readPacket(qp);
                switch(qp.getType()) {
                case Leader.PROPOSAL:
                    PacketInFlight pif = new PacketInFlight();
                    pif.hdr = new TxnHeader();
                    pif.rec = SerializeUtils.deserializeTxn(qp.getData(), pif.hdr);
                    if (pif.hdr.getZxid() != lastQueued + 1) {
                    LOG.warn("Got zxid 0x"
                            + Long.toHexString(pif.hdr.getZxid())
                            + " expected 0x"
                            + Long.toHexString(lastQueued + 1));
                    }
                    lastQueued = pif.hdr.getZxid();
                    
                    if (pif.hdr.getType() == OpCode.reconfig){                
                        SetDataTxn setDataTxn = (SetDataTxn) pif.rec;       
                       QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData()));
                       self.setLastSeenQuorumVerifier(qv, true);                               
                    }
                    
                    packetsNotCommitted.add(pif);
                    break;
                case Leader.COMMIT:
                case Leader.COMMITANDACTIVATE:
                    pif = packetsNotCommitted.peekFirst();
                    if (pif.hdr.getZxid() == qp.getZxid() && qp.getType() == Leader.COMMITANDACTIVATE) {
                        QuorumVerifier qv = self.configFromString(new String(((SetDataTxn) pif.rec).getData()));
                        boolean majorChange = self.processReconfig(qv, ByteBuffer.wrap(qp.getData()).getLong(),
                                qp.getZxid(), true);
                        if (majorChange) {
                            throw new Exception("changes proposed in reconfig");
                        }
                    }
                    if (!writeToTxnLog) {
                        if (pif.hdr.getZxid() != qp.getZxid()) {
                            LOG.warn("Committing " + qp.getZxid() + ", but next proposal is " + pif.hdr.getZxid());
                        } else {
                            zk.processTxn(pif.hdr, pif.rec);
                            packetsNotCommitted.remove();
                        }
                    } else {
                        packetsCommitted.add(qp.getZxid());
                    }
                    break;
                case Leader.INFORM:
                case Leader.INFORMANDACTIVATE:
                    PacketInFlight packet = new PacketInFlight();
                    packet.hdr = new TxnHeader();

                    if (qp.getType() == Leader.INFORMANDACTIVATE) {
                        ByteBuffer buffer = ByteBuffer.wrap(qp.getData());
                        long suggestedLeaderId = buffer.getLong();
                        byte[] remainingdata = new byte[buffer.remaining()];
                        buffer.get(remainingdata);
                        packet.rec = SerializeUtils.deserializeTxn(remainingdata, packet.hdr);
                        QuorumVerifier qv = self.configFromString(new String(((SetDataTxn)packet.rec).getData()));
                        boolean majorChange =
                                self.processReconfig(qv, suggestedLeaderId, qp.getZxid(), true);
                        if (majorChange) {
                            throw new Exception("changes proposed in reconfig");
                        }
                    } else {
                        packet.rec = SerializeUtils.deserializeTxn(qp.getData(), packet.hdr);
                        // Log warning message if txn comes out-of-order
                        if (packet.hdr.getZxid() != lastQueued + 1) {
                            LOG.warn("Got zxid 0x"
                                    + Long.toHexString(packet.hdr.getZxid())
                                    + " expected 0x"
                                    + Long.toHexString(lastQueued + 1));
                        }
                        lastQueued = packet.hdr.getZxid();
                    }
                    if (!writeToTxnLog) {
                        // Apply to db directly if we haven't taken the snapshot
                        zk.processTxn(packet.hdr, packet.rec);
                    } else {
                        packetsNotCommitted.add(packet);
                        packetsCommitted.add(qp.getZxid());
                    }

                    break;                
                case Leader.UPTODATE:
                    LOG.info("Learner received UPTODATE message");                                      
                    if (newLeaderQV!=null) {
                       boolean majorChange =
                           self.processReconfig(newLeaderQV, null, null, true);
                       if (majorChange) {
                           throw new Exception("changes proposed in reconfig");
                       }
                    }
                    if (isPreZAB1_0) {
                        zk.takeSnapshot();
                        self.setCurrentEpoch(newEpoch);
                    }
                    self.setZooKeeperServer(zk);
                    self.adminServer.setZooKeeperServer(zk);
                    break outerLoop;
                case Leader.NEWLEADER: // Getting NEWLEADER here instead of in discovery 
                    // means this is Zab 1.0
                   LOG.info("Learner received NEWLEADER message");
                   if (qp.getData()!=null && qp.getData().length > 1) {
                       try {                       
                           QuorumVerifier qv = self.configFromString(new String(qp.getData()));
                           self.setLastSeenQuorumVerifier(qv, true);
                           newLeaderQV = qv;
                       } catch (Exception e) {
                           e.printStackTrace();
                       }
                   }

                   if (snapshotNeeded) {
                       zk.takeSnapshot();
                   }
                   
                    self.setCurrentEpoch(newEpoch);
                    writeToTxnLog = true; //Anything after this needs to go to the transaction log, not applied directly in memory
                    isPreZAB1_0 = false;
                    writePacket(new QuorumPacket(Leader.ACK, newLeaderZxid, null, null), true);
                    break;
                }
            }
        }
        ack.setZxid(ZxidUtils.makeZxid(newEpoch, 0));
        writePacket(ack, true);
        sock.setSoTimeout(self.tickTime * self.syncLimit);
        zk.startup();
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(newEpoch);

        // We need to log the stuff that came in between the snapshot and the uptodate
        if (zk instanceof FollowerZooKeeperServer) {
            FollowerZooKeeperServer fzk = (FollowerZooKeeperServer)zk;
            for(PacketInFlight p: packetsNotCommitted) {
                fzk.logRequest(p.hdr, p.rec);
            }
            for(Long zxid: packetsCommitted) {
                fzk.commit(zxid);
            }
        } else if (zk instanceof ObserverZooKeeperServer) {
            // Similar to follower, we need to log requests between the snapshot
            // and UPTODATE
            ObserverZooKeeperServer ozk = (ObserverZooKeeperServer) zk;
            for (PacketInFlight p : packetsNotCommitted) {
                Long zxid = packetsCommitted.peekFirst();
                if (p.hdr.getZxid() != zxid) {
                    // log warning message if there is no matching commit
                    // old leader send outstanding proposal to observer
                    LOG.warn("Committing " + Long.toHexString(zxid)
                            + ", but next proposal is "
                            + Long.toHexString(p.hdr.getZxid()));
                    continue;
                }
                packetsCommitted.remove();
                Request request = new Request(null, p.hdr.getClientId(),
                        p.hdr.getCxid(), p.hdr.getType(), null, null);
                request.setTxn(p.rec);
                request.setHdr(p.hdr);
                ozk.commitRequest(request);
            }
        } else {
            // New server type need to handle in-flight packets
            throw new UnsupportedOperationException("Unknown server type");
        }
    }
    
    protected void revalidate(QuorumPacket qp) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(qp
                .getData());
        DataInputStream dis = new DataInputStream(bis);
        long sessionId = dis.readLong();
        boolean valid = dis.readBoolean();
        ServerCnxn cnxn = pendingRevalidations.remove(sessionId);
        if (cnxn == null) {
            LOG.warn("Missing session 0x"
                    + Long.toHexString(sessionId)
                    + " for validation");
        } else {
            zk.finishSessionInit(cnxn, valid);
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                    ZooTrace.SESSION_TRACE_MASK,
                    "Session 0x" + Long.toHexString(sessionId)
                    + " is valid: " + valid);
        }
    }
    // ping消息 返回 session Data
    protected void ping(QuorumPacket qp) throws IOException {
        // Send back the ping with our session data
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        Map<Long, Integer> touchTable = zk.getTouchSnapshot();
        for (Entry<Long, Integer> entry : touchTable.entrySet()) {
            dos.writeLong(entry.getKey());
            dos.writeInt(entry.getValue());
        }
        qp.setData(bos.toByteArray());
        writePacket(qp, true);
    }
    
    
    /**
     * Shutdown the Peer
     */
    public void shutdown() {
        self.setZooKeeperServer(null);
        self.closeAllConnections();
        self.adminServer.setZooKeeperServer(null);
        // shutdown previous zookeeper
        if (zk != null) {
            zk.shutdown();
        }
    }

    boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }
}
