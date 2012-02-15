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

import org.voltdb.StoredProcedureInvocation;

public class Iv2SPInitMessage extends VoltMessage
{

    private long m_connectionId;
    private StoredProcedureInvocation m_invocation;

    /** Serialization requires nullary ctor. */
    public Iv2SPInitMessage()
    {
        super();
    }

    public Iv2SPInitMessage(
        final long connectionId,
        final StoredProcedureInvocation invocation)
    {
        m_connectionId = connectionId;
        m_invocation = invocation;
    }

    public long connectionId()
    {
        return m_connectionId;
    }

    public StoredProcedureInvocation invocation()
    {
        return m_invocation;
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException
    {
        m_connectionId = buf.getLong();
        m_invocation.initFromBuffer(buf);
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.Iv2SP_INIT_MESSAGE_ID);
        buf.putLong(m_connectionId);
        m_invocation.flattenToBuffer(buf);
    }

    @Override
    public int getSerializedSize()
    {
        return
            super.getSerializedSize() +
            + 8 // connectionId
            + m_invocation.getSerializedSize()
            ;
    }
}
