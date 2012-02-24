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

import org.voltcore.messaging.VoltMessage;

/**
 * Initiate ack message. Used by the initiator replicas to ack the reception of
 * an initiate task message to the primary initiator.
 */
public class InitiateAckMessage extends VoltMessage {
    private long m_txnId;

    /** Empty constructor for de-serialization */
    public InitiateAckMessage() {
        super();
    }

    public InitiateAckMessage(long txnId) {
        m_txnId = txnId;
    }

    public long getTransactionId() {
        return m_txnId;
    }

    public void setTransactionId(long txnId) {
        m_txnId = txnId;
    }

    /* (non-Javadoc)
     * @see org.voltcore.messaging.VoltMessage#initFromBuffer(java.nio.ByteBuffer)
     */
    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        m_txnId = buf.getLong();
    }

    /* (non-Javadoc)
     * @see org.voltcore.messaging.VoltMessage#flattenToBuffer(java.nio.ByteBuffer)
     */
    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.INITIATE_ACK_ID);

        buf.putLong(m_txnId);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public int getSerializedSize() {
        return super.getSerializedSize() + 8;
    }

}
