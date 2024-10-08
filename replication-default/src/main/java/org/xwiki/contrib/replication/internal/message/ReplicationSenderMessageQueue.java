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
package org.xwiki.contrib.replication.internal.message;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.xwiki.component.annotation.Component;
import org.xwiki.component.annotation.InstantiationStrategy;
import org.xwiki.component.descriptor.ComponentInstantiationStrategy;
import org.xwiki.contrib.replication.ReplicationException;
import org.xwiki.contrib.replication.ReplicationInstance;
import org.xwiki.contrib.replication.ReplicationSenderMessage;
import org.xwiki.contrib.replication.event.ReplicationMessageSendingEvent;
import org.xwiki.contrib.replication.internal.ReplicationClient;
import org.xwiki.contrib.replication.internal.message.ReplicationSenderMessageStore.FileReplicationSenderMessage;
import org.xwiki.contrib.replication.internal.message.log.ReplicationMessageLogStore;
import org.xwiki.contrib.replication.log.ReplicationMessageEventQuery;
import org.xwiki.observation.ObservationManager;

/**
 * Maintain a queue of replication data to send to a specific instance.
 * 
 * @version $Id$
 */
@Component(roles = ReplicationSenderMessageQueue.class)
@InstantiationStrategy(ComponentInstantiationStrategy.PER_LOOKUP)
public class ReplicationSenderMessageQueue extends AbstractReplicationMessageQueue<ReplicationSenderMessage>
{
    @Inject
    private ReplicationSenderMessageStore store;

    @Inject
    private ReplicationClient client;

    @Inject
    private ObservationManager observation;

    @Inject
    private ReplicationMessageLogStore logStore;

    /**
     * Used to wait for a ping or a timeout.
     */
    private final ReentrantLock pingLock = new ReentrantLock();

    /**
     * Condition for waiting answer.
     */
    private final Condition pingCondition = this.pingLock.newCondition();

    private final Lock lock = new ReentrantLock();

    private ReplicationInstance instance;

    private int wait;

    private Throwable lastError;

    private Date nextTry;

    /**
     * @return the instance to send messages to
     */
    public ReplicationInstance getInstance()
    {
        return this.instance;
    }

    /**
     * @param instance the instance to send messages to
     */
    public void start(ReplicationInstance instance)
    {
        this.instance = instance;
        this.store.initialize(instance);

        initializeQueue();

        // Load the queue from disk
        Queue<ReplicationSenderMessage> messages = this.store.load();
        messages.forEach(this.queue::add);
    }

    @Override
    protected String getThreadName()
    {
        return "Replication message sending to [" + this.instance.getURI() + "]";
    }

    @Override
    protected void handle(ReplicationSenderMessage message) throws InterruptedException
    {
        // Notify that a message is about to be sent
        ReplicationMessageSendingEvent event = new ReplicationMessageSendingEvent();
        this.observation.notify(event, message, this.instance);
        if (event.isCanceled()) {
            this.logger.warn("The sending of the message with id [{}] and type [{}] was cancelled: {}", message.getId(),
                message.getType(), event.getReason());

            return;
        }

        // Try to send the message until it works
        while (true) {
            try {
                // Send the data to the instance
                this.client.sendMessage(message, this.instance);

                // Log the successfully sent message
                this.logStore.saveAsync(message, (m, e) -> {
                    Map<String, Object> custom = new HashMap<>(e.getCustom());

                    custom.put(ReplicationMessageEventQuery.KEY_STATUS, ReplicationMessageEventQuery.VALUE_STATUS_SENT);
                    custom.put(ReplicationMessageEventQuery.KEY_TARGET, this.instance.getURI());

                    e.setCustom(custom);
                });

                // Stop the loop
                break;
            } catch (Exception e) {
                // Remember the last error
                this.lastError = e;

                // Wait before trying to send the message again

                if (this.wait < 60) {
                    // Double the wait
                    // Start waiting at 1 minute
                    this.wait = this.wait == 0 ? 1 : this.wait * 2;
                } else {
                    // Wait a maximum of 2h (120 min)
                    this.wait = 120;
                }

                // Calculate next try date
                Calendar calendar = Calendar.getInstance();
                calendar.add(Calendar.MINUTE, this.wait);
                this.nextTry = calendar.getTime();

                this.logger.warn(
                    "Failed to send replication message with id [{}] and type [{}] to instance [{}],"
                        + " retrying in [{}] minutes: {}",
                    message.getId(), message.getType(), this.instance.getURI(), this.wait,
                    ExceptionUtils.getRootCauseMessage(e));

                // Wait
                this.pingLock.lockInterruptibly();
                try {
                    this.pingCondition.awaitUntil(this.nextTry);
                } finally {
                    this.pingLock.unlock();
                }

                // Make sure the current message is still the same (might have been purged while waiting)
                if (this.currentMessage != message) {
                    return;
                }
            }
        }

        // Reset the wait
        this.wait = 0;
        this.nextTry = null;
        // Reset the last error
        this.lastError = null;
    }

    /**
     * @return the last error catched when trying to send a message
     */
    public Throwable getLastError()
    {
        return this.lastError;
    }

    /**
     * @return the date when to try again the last failing message
     */
    public Date getNextTry()
    {
        return this.nextTry;
    }

    @Override
    protected void removeFromStore(ReplicationSenderMessage message) throws ReplicationException
    {
        this.store.delete(message);
    }

    /**
     * @param message the message to store and add to the queue
     * @return the stored message
     * @throws ReplicationException when failing to store the message
     */
    public FileReplicationSenderMessage add(ReplicationSenderMessage message) throws ReplicationException
    {
        // Serialize the data
        FileReplicationSenderMessage storedMessage;
        try {
            storedMessage = this.store.store(message);
        } catch (Exception e) {
            throw new ReplicationException("Failed to store sender message with id [" + message.getId() + "]", e);
        }

        this.queue.add(storedMessage);

        return storedMessage;
    }

    /**
     * Force the queue to resume sending messages.
     */
    public void wakeUp()
    {
        this.pingLock.lock();

        try {
            // Reset the wait
            this.wait = 0;
            this.nextTry = null;

            // Wake up the queue
            this.pingCondition.signal();
        } finally {
            this.pingLock.unlock();
        }
    }

    /**
     * Remove from the queue any waiting message to send.
     * 
     * @since 2.0.0
     */
    public void purge()
    {
        // Remove messages from the queue
        for (ReplicationSenderMessage message = this.queue.poll(); message != null; message = this.queue.poll()) {
            // Remove the message from the store
            removeFromStoreIgnoreException(message);
        }

        // Remember current message
        ReplicationSenderMessage message = this.currentMessage;

        if (message != null) {
            // Reset the current message if any
            this.currentMessage = null;

            // Remove the current message from the store
            removeFromStoreIgnoreException(message);

            // And reset the corresponding error if any
            this.lastError = null;

            // Make sure the queue is not waiting
            wakeUp();
        }
    }

    private void removeFromStoreIgnoreException(ReplicationSenderMessage message)
    {
        try {
            removeFromStore(message);
        } catch (ReplicationException e) {
            this.logger.error("Failed to remove the message from the fielsystem", e);
        }
    }
}
