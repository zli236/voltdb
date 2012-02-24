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

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.MiscUtils;
import org.voltdb.StoredProcedureInvocation;

/**
 * Message from an initiator to an execution site, instructing the
 * site to begin executing a stored procedure, coordinating other
 * execution sites if needed.
 *
 */
public class InitiateTaskMessage extends VoltMessage
{
    private long m_initiatorHSId;
    private long m_coordinatorHSId;
    private long m_transactionId;
    private long m_clientInterfaceHSId;
    private long m_clientInterfaceHandle;
    private boolean m_isReadOnly;
    private boolean m_isSinglePartition;
    private StoredProcedureInvocation m_invocation;

    // Sent by the primary initiator to the replicas for truncation
    private long m_truncationTxnId = -1;

    AtomicBoolean m_isDurable;

    /** Empty constructor for de-serialization */
    InitiateTaskMessage()
    {
        super();
    }

    public InitiateTaskMessage(
            long initiatorHSId,
            long coordinatorHSId,
            long txnId,
            long clientInterfaceHSId,
            long clientInterfaceHandle,
            boolean isReadOnly,
            boolean isSinglePartition,
            StoredProcedureInvocation invocation)
    {
        m_initiatorHSId = initiatorHSId;
        m_coordinatorHSId = coordinatorHSId;
        m_transactionId = txnId;
        m_clientInterfaceHSId = clientInterfaceHSId;
        m_clientInterfaceHandle = clientInterfaceHandle;
        m_isReadOnly = isReadOnly;
        m_isSinglePartition = isSinglePartition;
        m_invocation = invocation;
    }

    public long getInitiatorHSId()
    {
        return m_initiatorHSId;
    }

    // replica may set this to replica initiator HSId
    public void setInitiatorHSId(long HSId)
    {
        m_initiatorHSId = HSId;
    }

    public long getCoordinatorHSId()
    {
        return m_coordinatorHSId;
    }

    public long getTransactionId()
    {
        return m_transactionId;
    }

    // initiator sets txnid after clientinterface sends msg.
    public void setTransactionId(long txnId)
    {
        m_transactionId = txnId;
    }

    public long getClientInterfaceHSId()
    {
        return m_clientInterfaceHSId;
    }

    public long getClientInterfaceHandle()
    {
        return m_clientInterfaceHandle;
    }

    public boolean isReadOnly()
    {
        return m_isReadOnly;
    }

    public boolean isSinglePartition()
    {
        return m_isSinglePartition;
    }

    public StoredProcedureInvocation getStoredProcedureInvocation()
    {
        return m_invocation;
    }

    public String getStoredProcedureName()
    {
        return m_invocation.getProcName();
    }

    public int getParameterCount()
    {
        assert(m_invocation != null);
        if (m_invocation.getParams() == null)
            return 0;
        return m_invocation.getParams().toArray().length;
    }

    public Object[] getParameters()
    {
        return m_invocation.getParams().toArray();
    }

    public AtomicBoolean getDurabilityFlag()
    {
        assert(!m_isReadOnly);
        if (m_isDurable == null) {
            m_isDurable = new AtomicBoolean();
        }
        return m_isDurable;
    }

    public AtomicBoolean getDurabilityFlagIfItExists()
    {
        return m_isDurable;
    }

    public void setTruncationTxnId(long txnId)
    {
        m_truncationTxnId = txnId;
    }

    public long getTruncationTxnId()
    {
        return m_truncationTxnId;
    }

    @Override
    public int getSerializedSize()
    {
        return
            super.getSerializedSize()
            + 8 // initiatorHSId
            + 8 // coordinatorHSId
            + 8 // transactionId
            + 8 // clientInterfaceHSId
            + 8 // clientInterfaceHandle
            + 8 // truncationTxnId
            + 1 // isReadOnly
            + 1 // isSinglePartition
            + m_invocation.getSerializedSize()
            ;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.INITIATE_TASK_ID);

        buf.putLong(m_initiatorHSId);
        buf.putLong(m_coordinatorHSId);
        buf.putLong(m_transactionId);
        buf.putLong(m_clientInterfaceHSId);
        buf.putLong(m_clientInterfaceHandle);
        buf.putLong(m_truncationTxnId);
        buf.put(m_isReadOnly ? (byte) 1 : (byte) 0);
        buf.put(m_isSinglePartition ? (byte) 1 : (byte) 0);
        m_invocation.flattenToBuffer(buf);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        m_initiatorHSId = buf.getLong();
        m_coordinatorHSId = buf.getLong();
        m_transactionId = buf.getLong();
        m_clientInterfaceHSId = buf.getLong();
        m_clientInterfaceHandle = buf.getLong();
        m_truncationTxnId = buf.getLong();
        m_isReadOnly = buf.get() == 1;
        m_isSinglePartition = buf.get() == 1;

        m_invocation = new StoredProcedureInvocation();
        m_invocation.initFromBuffer(buf);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("INITITATE_TASK (FROM ");
        sb.append(MiscUtils.hsIdToString(getInitiatorHSId()));
        sb.append(" TO ");
        sb.append(MiscUtils.hsIdToString(getCoordinatorHSId()));
        sb.append(") FOR TXN ");
        sb.append(getTransactionId());

        sb.append("\n");
        if (m_isReadOnly)
            sb.append("  READ, ");
        else
            sb.append("  WRITE, ");
        if (m_isSinglePartition)
            sb.append("SINGLE PARTITION, ");
        else
            sb.append("MULTI PARTITION, ");
        sb.append("COORD ");
        sb.append(MiscUtils.hsIdToString(getCoordinatorHSId()));

        sb.append("\n  PROCEDURE: ");
        sb.append(m_invocation.getProcName());
        sb.append("\n  PARAMS: ");
        sb.append(m_invocation.getParams().toString());

        return sb.toString();
    }

    public ByteBuffer getSerializedParams()
    {
        return m_invocation.getSerializedParams();
    }
}
