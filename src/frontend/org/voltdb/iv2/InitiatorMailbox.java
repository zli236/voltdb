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

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.MessagingException;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;

import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

/**
 * InitiatorMailbox accepts initiator work and proxies it to the
 * configured InitiationRole.
 */
public class InitiatorMailbox implements Mailbox
{
    VoltLogger hostLog = new VoltLogger("HOST");
    private final int partitionId;
    private final HostMessenger messenger;
    private long hsId;

    // for now, there are only primary initiatiors.
    private InitiatorRole role = new PrimaryRole();

    public InitiatorMailbox(HostMessenger messenger, int partitionId)
    {
        this.messenger = messenger;
        this.partitionId = partitionId;
    }

    @Override
    public void send(long destHSId, VoltMessage message) throws MessagingException
    {
        message.m_sourceHSId = this.hsId;
        messenger.send(destHSId, message);
    }

    @Override
    public void send(long[] destHSIds, VoltMessage message) throws MessagingException
    {
        message.m_sourceHSId = this.hsId;
        messenger.send(destHSIds, message);
    }

    @Override
    public void deliver(VoltMessage message)
    {
        if (message instanceof InitiateTaskMessage) {
            role.offerInitiateTask((InitiateTaskMessage)message);
        }
        else if (message instanceof InitiateResponseMessage) {
            // this isn't quite right. But - offer to the role
            // and then always respond to the client interface.
            InitiateResponseMessage response = (InitiateResponseMessage)message;
            role.offerResponse(response);
            try {
                send(response.getClientInterfaceHSId(), response);
            }
            catch (MessagingException e) {
                hostLog.error("Failed to deliver response from execution site.", e);
            }
        }
    }

    @Override
    public VoltMessage recv()
    {
        return role.poll();
    }

    @Override
    public void deliverFront(VoltMessage message)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking()
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking(long timeout)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recv(Subject[] s)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking(Subject[] s)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking(Subject[] s, long timeout)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public long getHSId()
    {
        return hsId;
    }

    @Override
    public void setHSId(long hsId)
    {
        this.hsId = hsId;
    }


}
