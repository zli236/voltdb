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

import java.util.concurrent.LinkedBlockingDeque;


import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

public class PrimaryRole implements InitiatorRole {

    private long txnIdSequence = 0;

    private LinkedBlockingDeque<InitiateTaskMessage> initiations =
        new LinkedBlockingDeque<InitiateTaskMessage>();

    @Override
    public void offerInitiateTask(InitiateTaskMessage message)
    {
        message.setTransactionId(txnIdSequence++);
        initiations.offer((InitiateTaskMessage)message);
    }

    @Override
    public void offerResponse(InitiateResponseMessage message)
    {
    }

    @Override
    public InitiateTaskMessage poll()
    {
        return initiations.poll();
    }

}

