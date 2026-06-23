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
import java.util.concurrent.locks.ReentrantLock;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.xwiki.component.annotation.Component;
import org.xwiki.component.annotation.InstantiationStrategy;
import org.xwiki.component.descriptor.ComponentInstantiationStrategy;
import org.xwiki.component.manager.ComponentLifecycleException;
import org.xwiki.component.phase.Disposable;
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
public class ReplicationSenderMessageQueue implements Disposable, Runnable
{
    @Inject
    private ReplicationSenderMessageStore store;

    @Inject
    private ReplicationClient client;

    @Inject
    private ObservationManager observation;

    @Inject
    private ReplicationMessageLogStore logStore;

    @Inject
    private Logger logger;

    /**
     * Used to wait for a ping or a timeout.
     */
    private final ReentrantLock pingLock = new ReentrantLock();

    /**
     * Condition for waiting answer.
     */
    private final Condition pingCondition = this.pingLock.newCondition();

    private Thread thread;

    private ReplicationInstance instance;

    private int wait;

    private Throwable lastError;

    private Date nextTry;

    private boolean disposed;

    // It is volatile only for the reference, not for its content
    @SuppressWarnings("java:S3077")
    private volatile ReplicationSenderMessage headMessage;

    /**
     * @return the instance to send messages to
     */
    public ReplicationInstance getInstance()
    {
        return this.instance;
    }

    /**
     * @return the message currently being handled
     * @since 2.4.0
     */
    public ReplicationSenderMessage getCurrentMessage()
    {
        return this.headMessage;
    }

    /**
     * @param instance the instance to send messages to
     * @throws ReplicationException when failing to start the queue for the given instance
     */
    public void start(ReplicationInstance instance) throws ReplicationException
    {
        this.instance = instance;
        this.store.initialize(instance);

        // Initialize messages handling thread
        this.thread = new Thread(this);
        this.thread.setName("Replication message sending to [" + this.instance.getURI() + "]");
        this.thread.setPriority(Thread.NORM_PRIORITY - 2);
        // That thread can be stopped any time without really loosing anything
        this.thread.setDaemon(true);
        this.thread.start();
    }

    @Override
    public void dispose() throws ComponentLifecycleException
    {
        // Mark as disposed
        this.disposed = true;

        // Cancel and attempt to retry a failing message if any
        this.headMessage = null;

        // Wakup the thread if it is waiting for a failing message retry
        wakeUp();
    }

    @Override
    public void run()
    {
        while (!this.disposed) {
            try {
                this.headMessage = waitForMessage();

                if (this.headMessage == null) {
                    continue;
                }

                sendHeadMessage(this.headMessage);

                if (this.headMessage != null) {
                    this.store.removeHead();
                    this.headMessage = null;
                }
            } catch (InterruptedException e) {
                this.logger.warn("The replication sending thread has been interrupted");

                // Mark the thread as interrupted
                this.thread.interrupt();

                // Stop the loop
                break;
            } catch (Throwable t) {
                if (this.headMessage != null) {
                    this.logger.error(
                        "An unexpected throwable was thrown while sending message with id [{}] and type [{}]",
                        this.headMessage.getId(), this.headMessage.getType(), t);
                } else {
                    this.logger.error("An unexpected throwable was thrown while handling the replication sender queue",
                        t);
                }
            }
        }
    }

    private void sendHeadMessage(ReplicationSenderMessage message) throws InterruptedException
    {
        // Notify that a message is about to be sent
        ReplicationMessageSendingEvent event = new ReplicationMessageSendingEvent();
        this.observation.notify(event, message, this.instance);
        if (event.isCanceled()) {
            // A listener decided to not send the message
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

                this.nextTry = computeNextTry();

                this.logger.warn(
                    "Failed to send replication message with id [{}] and type [{}] to instance [{}],"
                        + " retrying in [{}] minutes: {}",
                    message.getId(), message.getType(), this.instance.getURI(), this.wait,
                    ExceptionUtils.getRootCauseMessage(e));

                // Wait
                this.pingLock.lockInterruptibly();
                try {
                    boolean awaitReturn = this.pingCondition.awaitUntil(this.nextTry);

                    if (awaitReturn) {
                        this.logger.debug(
                            "The wait for message with id [{}] and type [{}] stopped before reaching the timeout",
                            message.getId(), message.getType());
                    }
                } finally {
                    this.pingLock.unlock();
                }

                // Make sure the current message is still the same (might have been purged or disposed while waiting)
                if (this.headMessage != message) {
                    // Stop the loop
                    break;
                }
            }
        }

        // Reset the wait
        this.wait = 0;
        this.nextTry = null;
        // Reset the last error
        this.lastError = null;
    }

    private Date computeNextTry()
    {
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
        return calendar.getTime();
    }

    private FileReplicationSenderMessage waitForMessage() throws InterruptedException
    {
        while (!this.disposed) {
            FileReplicationSenderMessage message = this.store.peek();
            if (message != null) {
                return message;
            }

            this.pingLock.lockInterruptibly();
            try {
                message = this.store.peek();
                if (message != null) {
                    return message;
                }

                this.pingCondition.await();
            } finally {
                this.pingLock.unlock();
            }
        }

        return null;
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

    /**
     * @param message the message to store and add to the queue
     * @return the stored message
     * @throws ReplicationException when failing to store the message
     * @since 2.4.0
     */
    public FileReplicationSenderMessage add(ReplicationSenderMessage message) throws ReplicationException
    {
        FileReplicationSenderMessage fileMessage;

        // Store the message
        try {
            fileMessage = this.store.add(message);
        } catch (Exception e) {
            throw new ReplicationException("Failed to store sender message with id [" + message.getId() + "]", e);
        }

        // Notify the queue that a new message is available
        wakeUp();

        // Return the stored message
        return fileMessage;
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
        // Remove all messages from the queue store
        try {
            this.store.purge();
        } catch (ReplicationException e) {
            this.logger.warn("Failed to purge replication sender message queue for instance [{}]",
                this.instance != null ? this.instance.getURI() : null, e);
        }

        // Reset state (especially if the thread is currently waiting to re-send a failed message)
        this.headMessage = null;
        wakeUp();
    }

    /**
     * @return the number of messages in the queue
     * @since 2.4.0
     */
    public long getSize()
    {
        return this.store.getSize();
    }

    /**
     * Load all messages from the filesystem queue in memory and return them.
     * 
     * @return the messages in the queue, in memory
     */
    public Queue<ReplicationSenderMessage> loadMessages()
    {
        return this.store.load();
    }
}
