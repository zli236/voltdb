/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import org.voltcore.utils.Pair;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

/**
 * Primary initiator role. It assigns transaction IDs to new transactions, then
 * wait for acks for them from the replicas. Once a transaction is acked by all
 * replicas, it's safe for execution. For executed transactions, it waits for
 * responses from all replicas before forwarding the response to the client
 * interface.
 */
public class PrimaryRole implements InitiatorRole {
    // caller of offerInitiateTask() synchronizes on this, so it's okay
    private long txnIdSequence = 0;
    private volatile long lastExecutedTxnId = -1;

    private final LinkedBlockingDeque<InitiateTaskMessage> outstanding =
            new LinkedBlockingDeque<InitiateTaskMessage>();
    private final Map<Long, InFlightTxnState> pendingResponses =
            new ConcurrentHashMap<Long, InFlightTxnState>();

    // Txn IDs acked by each replica
    private final Map<Long, Long> ackedTxns =
            Collections.synchronizedMap(new HashMap<Long, Long>());
    private volatile long minLastAckedTxn = -1;

    @Override
    public void offerInitiateTask(InitiateTaskMessage message)
    {
        message.setTransactionId(txnIdSequence++);
        message.setTruncationTxnId(lastExecutedTxnId);
        int expectedResponses = ackedTxns.size() + 1; // plus the leader
        InFlightTxnState state = new InFlightTxnState(message, expectedResponses);
        pendingResponses.put(message.getTransactionId(), state);
        outstanding.offer(message);
    }

    @Override
    public Pair<Long, InitiateResponseMessage> offerResponse(InitiateResponseMessage message)
    {
        InFlightTxnState state = pendingResponses.get(message.getTxnId());
        if (state == null) {
            throw new RuntimeException("Response for transaction " + message.getTxnId() +
                                       " released before all replicas responded");
        }

        if (state.addResponse(message)) {
            pendingResponses.remove(message.getTxnId());
            return Pair.of(message.getClientInterfaceHSId(), message);
        }
        return null;
    }

    @Override
    public InitiateTaskMessage poll()
    {
        InitiateTaskMessage task = outstanding.peek();
        // only execute things that are acked by all replicas
        if (task != null) {
            if (ackedTxns.isEmpty() || task.getTransactionId() <= minLastAckedTxn) {
                long txnId = task.getTransactionId();
                if (txnId != lastExecutedTxnId + 1) {
                    throw new RuntimeException("Transaction missing, expecting transaction " +
                                               (lastExecutedTxnId + 1) + ", but got " + txnId);
                }
                lastExecutedTxnId = task.getTransactionId();
                outstanding.remove();
            }
            else {
                task = null;
            }
        }
        return task;
    }

    /**
     * Record acks from replica.
     *
     * @param HSId Replica HSId
     * @param txnId The transaction ID the replica has received.
     */
    public void ack(long HSId, long txnId)
    {
        Long lastAck = ackedTxns.get(HSId);
        if (lastAck == null) {
            throw new RuntimeException("Replicated initiator " + HSId + " is not known");
        }

        if (lastAck < txnId) {
            ackedTxns.put(HSId, txnId);
            long newMin = Collections.min(ackedTxns.values());
            if (newMin < minLastAckedTxn) {
                throw new RuntimeException("Acked transactions go backward from " +
                                           minLastAckedTxn + " to " + newMin);
            }
            minLastAckedTxn = newMin;
        }
    }

    /**
     * Set the HSIds of all the replicas of this partition.
     * @param replicas
     */
    public void setReplicas(long[] replicas)
    {
        HashSet<Long> keys = new HashSet<Long>(ackedTxns.keySet());
        Map<Long, Long> newReplicas = new HashMap<Long, Long>();
        for (long HSId : replicas) {
            boolean contains = keys.remove(HSId);
            if (!contains) {
                newReplicas.put(HSId, -1l);
            }
        }

        synchronized (ackedTxns) {
            ackedTxns.keySet().removeAll(keys);
            ackedTxns.putAll(newReplicas);
        }
    }
}

