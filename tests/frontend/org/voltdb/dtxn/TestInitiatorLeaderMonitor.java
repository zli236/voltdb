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

package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.agreement.LeaderElector;
import org.voltcore.zk.ZKTestBase;
import org.voltdb.VoltZK;
import org.voltdb.iv2.InitiatorLeaderMonitor;

import static org.junit.Assert.*;

public class TestInitiatorLeaderMonitor extends ZKTestBase {
    private final List<LeaderElector> electors = new ArrayList<LeaderElector>();

    @Before
    public void setUp() throws Exception {
        setUpZK(3);
        VoltZK.createPersistentZKNodes(getClient(0));
    }

    @After
    public void tearDown() throws Exception {
        for (LeaderElector elector : electors) {
            try {
                elector.shutdown();
            } catch (KeeperException e) {}
        }
        electors.clear();
        tearDownZK();
    }

    private LeaderElector getElector(ZooKeeper zk, String path, String prefix) {
        LeaderElector elector = new LeaderElector(zk, path, prefix, new byte[0], null);
        electors.add(elector);
        return elector;
    }

    @Test
    public void testSimple() throws Exception {
        ZooKeeper zk = getClient(0);
        ZooKeeper zk2 = getClient(1);
        ZooKeeper zk3 = getClient(2);

        // create a single partition
        String path = VoltZK.electionDirForPartition(0);
        zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        LeaderElector elector1 = getElector(zk, path, "1");
        LeaderElector elector2 = getElector(zk2, path, "2");
        LeaderElector elector3 = getElector(zk3, path, "3");
        elector1.start(false);
        elector2.start(false);
        elector3.start(false);

        InitiatorLeaderMonitor monitor = new InitiatorLeaderMonitor(zk);
        monitor.start();
        assertEquals(1, monitor.getLeader(0).longValue());
    }

    @Test
    public void testLeaderChange() throws Exception {
        ZooKeeper zk = getClient(0);
        ZooKeeper zk2 = getClient(1);
        ZooKeeper zk3 = getClient(2);

        // create a single partition
        String path = VoltZK.electionDirForPartition(0);
        zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        LeaderElector elector1 = getElector(zk, path, "1");
        LeaderElector elector2 = getElector(zk2, path, "2");
        LeaderElector elector3 = getElector(zk3, path, "3");
        elector1.start(false);
        elector2.start(false);
        elector3.start(false);

        InitiatorLeaderMonitor monitor = new InitiatorLeaderMonitor(zk);
        monitor.start();
        assertEquals(1, monitor.getLeader(0).longValue());

        // leader is down, switch to new leader
        elector1.shutdown();
        Thread.sleep(50);
        assertEquals(2, monitor.getLeader(0).longValue());
    }

    @Test
    public void testNonLeaderChange() throws Exception {
        ZooKeeper zk = getClient(0);
        ZooKeeper zk2 = getClient(1);
        ZooKeeper zk3 = getClient(2);

        // create a single partition
        String path = VoltZK.electionDirForPartition(0);
        zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        LeaderElector elector1 = getElector(zk, path, "1");
        LeaderElector elector2 = getElector(zk2, path, "2");
        LeaderElector elector3 = getElector(zk3, path, "3");
        elector1.start(false);
        elector2.start(false);
        elector3.start(false);

        InitiatorLeaderMonitor monitor = new InitiatorLeaderMonitor(zk);
        monitor.start();
        assertEquals(1, monitor.getLeader(0).longValue());

        // non-leader is down, leader doesn't change
        elector2.shutdown();
        Thread.sleep(50);
        assertEquals(1, monitor.getLeader(0).longValue());
    }

    @Test
    public void testAddPartition() throws Exception {
        ZooKeeper zk = getClient(0);
        ZooKeeper zk2 = getClient(1);
        ZooKeeper zk3 = getClient(2);

        // create a single partition
        String path = VoltZK.electionDirForPartition(0);
        zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        LeaderElector elector1 = getElector(zk, path, "1");
        LeaderElector elector2 = getElector(zk2, path, "2");
        LeaderElector elector3 = getElector(zk3, path, "3");
        String path2 = VoltZK.electionDirForPartition(1);
        LeaderElector elector4 = getElector(zk, path2, "4");
        elector1.start(false);
        elector2.start(false);
        elector3.start(false);

        InitiatorLeaderMonitor monitor = new InitiatorLeaderMonitor(zk);
        monitor.start();
        assertEquals(1, monitor.getLeader(0).longValue());
        assertNull(monitor.getLeader(1));

        // add a partition
        zk.create(path2, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        elector4.start(true);

        Thread.sleep(50);
        assertNotNull(monitor.getLeader(1));
        assertEquals(4, monitor.getLeader(1).longValue());
    }

    @Test
    public void testRemovePartition() throws Exception {
        ZooKeeper zk = getClient(0);
        ZooKeeper zk2 = getClient(1);
        ZooKeeper zk3 = getClient(2);

        // create two partitions
        String path = VoltZK.electionDirForPartition(0);
        zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String path2 = VoltZK.electionDirForPartition(1);
        zk.create(path2, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        LeaderElector elector1 = getElector(zk, path, "1");
        LeaderElector elector2 = getElector(zk2, path, "2");
        LeaderElector elector3 = getElector(zk3, path, "3");
        LeaderElector elector4 = getElector(zk, path2, "4");
        elector1.start(false);
        elector2.start(false);
        elector3.start(false);
        elector4.start(false);

        InitiatorLeaderMonitor monitor = new InitiatorLeaderMonitor(zk2);
        monitor.start();
        assertEquals(1, monitor.getLeader(0).longValue());
        assertEquals(4, monitor.getLeader(1).longValue());

        // close zk will remove a partition and change leader of partition 0
        zk.close();

        Thread.sleep(50);
        assertNull(monitor.getLeader(1));
        assertEquals(2, monitor.getLeader(0).longValue());
    }
}
