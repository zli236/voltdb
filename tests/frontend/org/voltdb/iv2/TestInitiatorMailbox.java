/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.iv2;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.voltcore.agreement.LeaderElector;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.zk.ZKTestBase;
import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.InitiateAckMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

public class TestInitiatorMailbox extends ZKTestBase {
    private static VoltDBInterface mockVolt;
    private ZooKeeper zk;
    private HostMessenger hm = mock(HostMessenger.class);
    private InitiatorMailbox mb;

    @BeforeClass
    public static void setUpBeforeClass() {
        mockVolt = mock(VoltDBInterface.class);
        VoltDB.replaceVoltDBInstanceForTest(mockVolt);
    }

    @Before
    public void setUp() throws Exception {
        // trick to get around the real crash. This will throw a runtime exception
        doThrow(new RuntimeException("Crash VoltDB")).when(mockVolt).ignoreCrash();

        setUpZK(1);
        zk = getClient(0);
        VoltZK.createPersistentZKNodes(zk);
        doReturn(zk).when(hm).getZK();
        mb = new InitiatorMailbox(hm, 0);
        mb.setHSId(100);
    }

    @After
    public void tearDown() throws Exception {
        mb.shutdown();
        tearDownZK();
        reset(hm);
        reset(mockVolt);
    }

    private void createElectionNodes(int count, boolean isPrimary) {
        List<String> children = new ArrayList<String>(count);
        children.add(String.format("100_%10d", 0));
        for (int i = 0; i < count - 1; i++) {
            children.add(String.format("%d_%10d", i, i));
        }
        if (isPrimary) {
            mb.membershipChangeHandler.run(children);
        }
    }

    /**
     * @throws InterruptedException
     */
    private void generateTasks(final boolean isPrimary) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(3);
        final Object lock = new Object();
        final AtomicLong txnId = new AtomicLong();
        Runnable r = new Runnable() {
            @Override
            public void run() {
                latch.countDown();
                synchronized (lock) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {}

                    StoredProcedureInvocation procedure = new StoredProcedureInvocation();
                    procedure.setProcName("TestProc");
                    for (int i = 0; i < 1000; i++) {
                        InitiateTaskMessage message;
                        if (isPrimary) {
                            message = new InitiateTaskMessage(100, 100, -1, 0, 0, false, true, procedure);
                        } else {
                            message = new InitiateTaskMessage(100, 100, txnId.getAndIncrement(),
                                                              0, 0, false, true, procedure);
                        }
                        mb.deliver(message);
                    }
                }
            }
        };

        Thread thread1 = new Thread(r);
        Thread thread2 = new Thread(r);
        Thread thread3 = new Thread(r);
        thread1.start();
        thread2.start();
        thread3.start();

        // start all threads together
        latch.await();
        synchronized (lock) {
            lock.notifyAll();
        }

        thread1.join();
        thread2.join();
        thread3.join();
    }

    /**
     * Test 3 threads concurrently deliver 3000 initiate task messages to the
     * mailbox.
     *
     * @throws Exception
     */
    @Test
    public void testConcurrentDelivery() throws Exception {
        mb.start();
        createElectionNodes(2, true);
        generateTasks(true);

        /*
         * check if the transactions are replicated to the replicas in the
         * correct order
         */
        ArgumentCaptor<InitiateTaskMessage> captor = ArgumentCaptor.forClass(InitiateTaskMessage.class);
        verify(hm, times(3000)).send(any(long[].class), captor.capture());
        List<InitiateTaskMessage> messages = captor.getAllValues();
        long txnId = 0;
        for (InitiateTaskMessage task : messages) {
            assertEquals(txnId++, task.getTransactionId());
        }
    }

    @Test
    public void testAck() throws Exception {
        mb.start();
        createElectionNodes(2, true);
        generateTasks(true);

        // no task until acked
        assertNull(mb.recv());

        // ack the first 1000
        InitiateAckMessage ackMessage1 = new InitiateAckMessage(999);
        ackMessage1.m_sourceHSId = 0;
        mb.deliver(ackMessage1);

        /*
         * check if the execution site can get the transactions in the correct
         * order
         */
        InitiateTaskMessage localTask;
        long localTxnId = 0;
        while ((localTask = (InitiateTaskMessage) mb.recv()) != null) {
            assertEquals(localTxnId++, localTask.getTransactionId());
        }
        assertEquals(1000, localTxnId);

        // ack the first 1000
        InitiateAckMessage ackMessage2 = new InitiateAckMessage(2999);
        ackMessage2.m_sourceHSId = 0;
        mb.deliver(ackMessage2);

        /*
         * check if the execution site can get the transactions in the correct
         * order
         */
        InitiateTaskMessage localTask2;
        long localTxnId2 = 1000;
        while ((localTask2 = (InitiateTaskMessage) mb.recv()) != null) {
            assertEquals(localTxnId2++, localTask2.getTransactionId());
        }
        assertEquals(3000, localTxnId2);
    }

    @Test
    public void testRespond() throws Exception {
        mb.start();
        createElectionNodes(2, true);
        generateTasks(true);
        VoltTable[] results =
                new VoltTable[] {new VoltTable(new ColumnInfo("T", VoltType.BIGINT))};

        /*
         * sometimes responses may come before local execution site has polled
         * the tasks. Return responses for the first 1000 transactions from the
         * replica
         */
        for (int i = 0; i < 1000; i++) {
            InitiateTaskMessage task =
                    new InitiateTaskMessage(100, 100, i, 0, 0, false, true, null);
            InitiateResponseMessage remoteResponse =
                    new InitiateResponseMessage(task);
            ClientResponseImpl resp = new ClientResponseImpl(ClientResponse.SUCCESS, results, "");
            remoteResponse.setResults(resp);
            mb.deliver(remoteResponse);
        }

        // ack all
        InitiateAckMessage ackMessage = new InitiateAckMessage(2999);
        ackMessage.m_sourceHSId = 0;
        mb.deliver(ackMessage);

        // return all responses from local execution site
        InitiateTaskMessage localTask;
        while ((localTask = (InitiateTaskMessage) mb.recv()) != null) {
            InitiateResponseMessage localResponse =
                    new InitiateResponseMessage(localTask);
            ClientResponseImpl resp = new ClientResponseImpl(ClientResponse.SUCCESS, results, "");
            localResponse.setResults(resp);
            mb.deliver(localResponse);
        }

        // the first 1000 tasks should be returned to the client interface
        ArgumentCaptor<InitiateResponseMessage> responseCaptor1 =
                ArgumentCaptor.forClass(InitiateResponseMessage.class);
        verify(hm, times(1000)).send(anyLong(), responseCaptor1.capture());
        List<InitiateResponseMessage> allValues = responseCaptor1.getAllValues();
        for (InitiateResponseMessage resp : allValues) {
            assertTrue(resp.getTxnId() < 1000);
        }

        // reset HostMessenger mock here so that we don't get previous stats
        reset(hm);

        /*
         * return the rest of the responses from the replica
         */
        for (int i = 1000; i < 3000; i++) {
            InitiateTaskMessage task =
                    new InitiateTaskMessage(100, 100, i, 0, 0, false, true, null);
            InitiateResponseMessage remoteResponse =
                    new InitiateResponseMessage(task);
            ClientResponseImpl resp = new ClientResponseImpl(ClientResponse.SUCCESS, results, "");
            remoteResponse.setResults(resp);
            mb.deliver(remoteResponse);
        }

        // the remaining 2000 tasks should be returned to the client interface
        ArgumentCaptor<InitiateResponseMessage> responseCaptor2 =
                ArgumentCaptor.forClass(InitiateResponseMessage.class);
        verify(hm, times(2000)).send(anyLong(), responseCaptor2.capture());
        List<InitiateResponseMessage> remainingResponses = responseCaptor2.getAllValues();
        for (InitiateResponseMessage resp : remainingResponses) {
            assertTrue(Long.toString(resp.getTxnId()), resp.getTxnId() >= 1000);
            assertTrue(resp.getTxnId() < 3000);
        }
    }

    @Test
    public void testResultLengthMismatch() throws Exception {
        mb.start();
        createElectionNodes(2, true);
        generateTasks(true);
        VoltTable[] results =
                new VoltTable[] {new VoltTable(new ColumnInfo("T", VoltType.BIGINT))};
        VoltTable[] emptyResults = new VoltTable[0];

        // replica response
        StoredProcedureInvocation procedure = new StoredProcedureInvocation();
        procedure.setProcName("TestProc");
        InitiateTaskMessage task =
                new InitiateTaskMessage(100, 100, 0, 0, 0, false, true, procedure);
        InitiateResponseMessage remoteResponse =
                new InitiateResponseMessage(task);
        ClientResponseImpl resp = new ClientResponseImpl(ClientResponse.SUCCESS, results, "");
        remoteResponse.setResults(resp);
        mb.deliver(remoteResponse);

        // local response
        InitiateResponseMessage localResponse = new InitiateResponseMessage(task);
        ClientResponseImpl resp2 = new ClientResponseImpl(ClientResponse.SUCCESS, emptyResults, "");
        localResponse.setResults(resp2);
        try {
            mb.deliver(localResponse);
        } catch (RuntimeException e) {
            return;
        }
        fail("Should throw an exception earlier");
    }

    @Test
    public void testResultMismatch() throws Exception {
        mb.start();
        createElectionNodes(2, true);
        generateTasks(true);
        VoltTable[] results =
                new VoltTable[] {new VoltTable(new ColumnInfo("T", VoltType.BIGINT))};
        VoltTable[] results2 = new VoltTable[] {new VoltTable(new ColumnInfo("A", VoltType.STRING))};

        // replica response
        StoredProcedureInvocation procedure = new StoredProcedureInvocation();
        procedure.setProcName("TestProc");
        InitiateTaskMessage task =
                new InitiateTaskMessage(100, 100, 0, 0, 0, false, true, procedure);
        InitiateResponseMessage remoteResponse =
                new InitiateResponseMessage(task);
        ClientResponseImpl resp = new ClientResponseImpl(ClientResponse.SUCCESS, results, "");
        remoteResponse.setResults(resp);
        mb.deliver(remoteResponse);

        // local response
        InitiateResponseMessage localResponse = new InitiateResponseMessage(task);
        ClientResponseImpl resp2 = new ClientResponseImpl(ClientResponse.SUCCESS, results2, "");
        localResponse.setResults(resp2);
        try {
            mb.deliver(localResponse);
        } catch (RuntimeException e) {
            return;
        }
        fail("Should throw an exception earlier");
    }

    @Test
    public void testReplicaAck() throws Exception {
        LeaderElector elector = new LeaderElector(hm.getZK(),
                                                  VoltZK.electionDirForPartition(0),
                                                  "0", null, null);
        // to prevent the initiator become the leader
        elector.start(true);
        mb.start();
        createElectionNodes(2, false);
        generateTasks(false);

        /*
         * check if the last ack received from the replica is the last transaction
         */
        ArgumentCaptor<InitiateAckMessage> captor = ArgumentCaptor.forClass(InitiateAckMessage.class);
        verify(hm, atLeastOnce()).send(anyLong(), captor.capture());
        List<InitiateAckMessage> messages = captor.getAllValues();
        assertEquals(2999, messages.get(messages.size() - 1).getTransactionId());
    }

    @Test
    public void testReplicaTruncation() throws Exception {
        LeaderElector elector = new LeaderElector(hm.getZK(),
                                                  VoltZK.electionDirForPartition(0),
                                                  "0", null, null);
        // to prevent the initiator become the leader
        elector.start(true);
        mb.start();
        createElectionNodes(2, false);
        generateTasks(false);

        // execute all transactions
        VoltTable[] results =
                new VoltTable[] {new VoltTable(new ColumnInfo("T", VoltType.BIGINT))};
        InitiateTaskMessage localTask;
        while ((localTask = (InitiateTaskMessage) mb.recv()) != null) {
            InitiateResponseMessage localResponse =
                    new InitiateResponseMessage(localTask);
            ClientResponseImpl resp = new ClientResponseImpl(ClientResponse.SUCCESS, results, "");
            localResponse.setResults(resp);
            mb.deliver(localResponse);
        }

        // nothing should be truncated
        ReplicatedRole role = (ReplicatedRole) mb.role;
        assertEquals(0, role.getOldestInFlightTxnId());

        InitiateTaskMessage message =
                new InitiateTaskMessage(100, 100, 3000, 0, 0, false, true, null);
        message.setTruncationTxnId(1000);
        mb.deliver(message);
        // anything before transaction 1001 should be removed
        assertEquals(1001, role.getOldestInFlightTxnId());
    }
}
