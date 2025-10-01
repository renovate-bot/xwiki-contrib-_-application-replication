/*
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.xwiki.contrib.replication.internal.message.question;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.xwiki.component.annotation.Component;
import org.xwiki.contrib.replication.AbstractReplicationReceiver;
import org.xwiki.contrib.replication.ReplicationException;
import org.xwiki.contrib.replication.ReplicationMessage;
import org.xwiki.contrib.replication.ReplicationReceiverMessage;
import org.xwiki.contrib.replication.internal.message.ReplicationReceiverMessageEvent;
import org.xwiki.observation.ObservationManager;

/**
 * @version $Id$
 * @since 2.0.0
 */
@Component
@Singleton
@Named(ReplicationMessage.TYPE_ANSWER)
public class ReplicationAnswerReceiver extends AbstractReplicationReceiver
{
    @Inject
    private ReplicationAnswerManager answers;

    @Inject
    private ObservationManager observation;

    @Override
    public void receive(ReplicationReceiverMessage message) throws ReplicationException
    {
        // Handler the answer
        this.answers.onReceive(message);

        // Inform other cluster members of the received answer
        this.observation.notify(new ReplicationReceiverMessageEvent(message.getType()), message);
    }
}
