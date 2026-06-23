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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.xwiki.component.annotation.Component;
import org.xwiki.component.annotation.InstantiationStrategy;
import org.xwiki.component.descriptor.ComponentInstantiationStrategy;
import org.xwiki.component.phase.Initializable;
import org.xwiki.component.phase.InitializationException;
import org.xwiki.contrib.replication.ReplicationException;
import org.xwiki.contrib.replication.ReplicationInstance;
import org.xwiki.contrib.replication.ReplicationSenderMessage;
import org.xwiki.contrib.replication.event.ReplicationMessageStoringEvent;
import org.xwiki.contrib.replication.internal.WrappingMutableReplicationSenderMessage;
import org.xwiki.observation.ObservationManager;

/**
 * @version $Id$
 */
@Component(roles = ReplicationSenderMessageStore.class)
@InstantiationStrategy(ComponentInstantiationStrategy.PER_LOOKUP)
public class ReplicationSenderMessageStore extends AbstractReplicationMessageStore<ReplicationSenderMessage>
    implements Initializable
{
    private static final String FILENAME_QUEUE_INDEX = "queue.index";

    private static final String FILENAME_QUEUE_HEAD = "queue.head";

    private static final String FILE_READ_ACCESS = "r";

    // 10MB
    private static final long COMPACT_INDEX_MIN_SIZE = 10L * 1024 * 1024;

    @Inject
    private ObservationManager observation;

    @Inject
    private Provider<WrappingMutableReplicationSenderMessage> wrappingMessageProvider;

    private final ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();

    private ReplicationInstance instance;

    private final class MessageIdDateEntry implements Comparable<MessageIdDateEntry>
    {
        private final String messageId;

        private final Date date;

        private MessageIdDateEntry(String messageId, Date date)
        {
            this.messageId = messageId;
            this.date = date;
        }

        @Override
        public int compareTo(MessageIdDateEntry other)
        {
            return this.date.compareTo(other.date);
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }

            if (other instanceof MessageIdDateEntry) {
                MessageIdDateEntry otherEntry = (MessageIdDateEntry) other;

                return this.messageId.equals(otherEntry.messageId) && this.date.equals(otherEntry.date);
            }

            return false;
        }

        @Override
        public int hashCode()
        {
            HashCodeBuilder builder = new HashCodeBuilder();

            builder.append(this.messageId);
            builder.append(this.date);

            return builder.toHashCode();
        }
    }

    /**
     * The message to send and the instance to send it to stored on the filesystem.
     * 
     * @version $Id$
     */
    public final class FileReplicationSenderMessage extends AbstractFileReplicationMessage
        implements ReplicationSenderMessage
    {
        private FileReplicationSenderMessage(File messageFolder) throws ConfigurationException, ReplicationException
        {
            super(messageFolder);
        }

        @Override
        public void write(OutputStream stream) throws IOException
        {
            FileUtils.copyFile(this.dataFile, stream);
        }
    }

    private File getQueueIndexFile()
    {
        return new File(this.home, FILENAME_QUEUE_INDEX);
    }

    private File getQueueHeadFile()
    {
        return new File(this.home, FILENAME_QUEUE_HEAD);
    }

    private long readHeadOffset() throws IOException
    {
        File headFile = getQueueHeadFile();
        if (!headFile.exists() || headFile.length() == 0) {
            return 0;
        }

        String value = Files.readString(headFile.toPath(), StandardCharsets.UTF_8).trim();
        return value.isEmpty() ? 0 : Long.parseLong(value);
    }

    private void writeHeadOffset(long offset) throws IOException
    {
        Files.writeString(getQueueHeadFile().toPath(), Long.toString(offset), StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static final class QueueEntry
    {
        private final String messageId;

        private final long nextOffset;

        private QueueEntry(String messageId, long nextOffset)
        {
            this.messageId = messageId;
            this.nextOffset = nextOffset;
        }
    }

    private QueueEntry readHeadEntry() throws IOException
    {
        // Get the index file
        File indexFile = getQueueIndexFile();
        if (!indexFile.exists() || indexFile.length() == 0) {
            return null;
        }

        // Get current offset
        long offset = readHeadOffset();
        if (offset >= indexFile.length()) {
            return null;
        }

        try (RandomAccessFile raf = new RandomAccessFile(indexFile, FILE_READ_ACCESS)) {
            // Skip beginning of the file until the current offset
            raf.seek(offset);

            // Read the current message id
            String line;
            while ((line = raf.readLine()) != null) {
                long nextOffset = raf.getFilePointer();

                String messageId = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
                if (StringUtils.isNotBlank(messageId)) {
                    return new QueueEntry(messageId, nextOffset);
                }

                offset = nextOffset;
            }
        }

        // No id remaining
        return null;
    }

    @Override
    public void initialize() throws InitializationException
    {
        setHome(new File(this.fileStore.getReplicationFolder(), "sender"));
    }

    private void migrateIndexIfNeeded() throws IOException
    {
        // Get the index file
        File indexFile = getQueueIndexFile();
        if (indexFile.exists()) {
            // If the index file already exists, there is no need to migrate the store
            return;
        }

        // Generate the index file from the existing messages in the store
        if (this.home.exists()) {
            // Gather ids of existing messages in the store and sort them by date
            Queue<MessageIdDateEntry> queue = new PriorityQueue<>();
            for (File file : this.home.listFiles()) {
                if (file.isDirectory()) {
                    try {
                        FileReplicationSenderMessage message = createReplicationMessage(file);
                        queue.offer(new MessageIdDateEntry(message.getId(), message.getDate()));
                    } catch (ReplicationException e) {
                        this.logger.error("Failed to load replication message from folder [{}]", file.getAbsolutePath(),
                            e);
                    }
                }
            }

            // Write the index file
            try (BufferedWriter writer = Files.newBufferedWriter(indexFile.toPath(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                while (!queue.isEmpty()) {
                    MessageIdDateEntry entry = queue.poll();
                    writer.write(entry.messageId);
                    writer.newLine();
                }
            }
        }
    }

    /**
     * @param instance the instance to send messages to
     * @throws ReplicationException when failing to initialize the store for the given instance
     * @since 2.0.0
     */
    public void initialize(ReplicationInstance instance) throws ReplicationException
    {
        this.instance = instance;

        setHome(new File(this.home, clean(this.instance.getURI())));

        // Generate the index file if it does not exist but there are messages in the store. This can happen when
        // upgrading from a version that did not have an index file.
        try {
            migrateIndexIfNeeded();
        } catch (IOException e) {
            throw new ReplicationException(
                "Failed to migrate the sender message store index for instance [" + instance.getURI() + "]", e);
        }
    }

    private String clean(String uri)
    {
        return StringUtils.replaceChars(uri, "/:@", "-_.");
    }

    @Override
    protected FileReplicationSenderMessage createReplicationMessage(File messageFolder) throws ReplicationException
    {
        try {
            return new FileReplicationSenderMessage(messageFolder);
        } catch (Exception e) {
            throw new ReplicationException(
                "Failed to create a file based ReplicationReceiverMessage instance from folder [" + messageFolder + "]",
                e);
        }
    }

    @Override
    protected void storeData(ReplicationSenderMessage message, File dataFile) throws IOException
    {
        try (FileOutputStream stream = new FileOutputStream(dataFile)) {
            message.write(stream);
        }
    }

    /**
     * @return the next message to send without removing it from the store
     * @since 2.4.0
     */
    public FileReplicationSenderMessage peek()
    {
        try {
            this.indexLock.readLock().lock();

            QueueEntry entry = readHeadEntry();
            if (entry == null) {
                return null;
            }

            return createReplicationMessage(getMessageFolder(entry.messageId));
        } catch (Exception e) {
            this.logger.error("Failed to peek the next sender message from queue index", e);
            return null;
        } finally {
            this.indexLock.readLock().unlock();
        }
    }

    private void adddToIndex(String messageId)
    {
        try {
            this.indexLock.writeLock().lock();

            // Make sure the home folder exists
            Files.createDirectories(this.home.toPath());

            // Append the message id to the index file
            try (BufferedWriter writer = Files.newBufferedWriter(getQueueIndexFile().toPath(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                writer.write(messageId);
                writer.newLine();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to add message [" + messageId + "] to the queue index", e);
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    /**
     * @param message the message to store
     * @return the new instance to manipulate the stored message
     * @throws ReplicationException when failing to store the message
     * @since 2.4.0
     */
    public FileReplicationSenderMessage add(ReplicationSenderMessage message) throws ReplicationException
    {
        // Give a change to customize the message to store
        WrappingMutableReplicationSenderMessage customMessage = this.wrappingMessageProvider.get();
        customMessage.initialize(message);
        this.observation.notify(new ReplicationMessageStoringEvent(), customMessage);

        // Store the message
        storeMessage(customMessage);

        // Add the message to the queue file
        try {
            adddToIndex(customMessage.getId());
        } catch (Exception e) {
            try {
                // Remove the message. It will never be sent since it's not part of the index.
                delete(customMessage.getId());
            } catch (Exception cleanException) {
                cleanException.addSuppressed(new ReplicationException(
                    "Failed to remove message [" + message.getId() + "] from the queue index", e));
                throw cleanException;
            }
        }

        // Return the message to manipulate
        return createReplicationMessage(getMessageFolder(customMessage.getId()));
    }

    private void compactIndexIfNeeded() throws IOException
    {
        // Get the index file
        File indexFile = getQueueIndexFile();
        if (!indexFile.exists()) {
            // No need to compact if the index file does not exist
            return;
        }

        // Get the current offset
        long headOffset = readHeadOffset();
        if (headOffset == 0) {
            // No need to compact if the head is at the beginning of the file already
            return;
        }

        // Get the index file length
        long fileLength = indexFile.length();

        // Compact when the file is bigger than the configured size and at least half it is obsolete.
        if (fileLength <= COMPACT_INDEX_MIN_SIZE || headOffset < fileLength / 2) {
            return;
        }

        // Compact the index
        compactIndex(indexFile, headOffset);
    }

    private void compactIndex(File indexFile, long headOffset) throws IOException
    {
        // Copy remaining queue entries to a temporary file
        File tempFile = new File(indexFile.getParentFile(), indexFile.getName() + ".tmp");
        try (RandomAccessFile in = new RandomAccessFile(indexFile, FILE_READ_ACCESS)) {
            in.seek(headOffset);

            try (OutputStream out = new FileOutputStream(tempFile)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = in.read(buffer)) != -1) {
                    out.write(buffer, 0, read);
                }
            }
        }

        // Replace the index file with the new one
        Files.move(tempFile.toPath(), indexFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        // Reset the offset
        writeHeadOffset(0);
    }

    /**
     * Remove the head of the queue of messages to send.
     * 
     * @throws ReplicationException when failing to remove the head of the queue
     * @since 2.4.0
     */
    public void removeHead() throws ReplicationException
    {
        try {
            this.indexLock.writeLock().lock();

            // Get and remove the head of the queue from the index
            String messageId = null;
            try {
                QueueEntry entry = readHeadEntry();
                if (entry != null) {
                    writeHeadOffset(entry.nextOffset);
                    compactIndexIfNeeded();

                    messageId = entry.messageId;
                }
            } catch (Exception e) {
                throw new ReplicationException("Failed to remove the head message from the queue index", e);
            }

            if (messageId != null) {
                // Remove the message from the store
                delete(messageId);
            }
        } finally {
            this.indexLock.writeLock().unlock();
        }
    }

    /**
     * Remove all queued sender messages from the store.
     *
     * @throws ReplicationException when failing to purge the queue
     * @since 2.4.0
     */
    public void purge() throws ReplicationException
    {
        // There is nothing to purge if the store is not initialized with an instance yet
        if (this.instance != null) {
            try {
                this.indexLock.writeLock().lock();

                if (this.home.exists()) {
                    FileUtils.deleteDirectory(this.home);
                }
            } catch (Exception e) {
                throw new ReplicationException("Failed to purge sender message queue", e);
            } finally {
                this.indexLock.writeLock().unlock();
            }
        }
    }

    /**
     * @return the number of queued sender messages, including the current head if any
     * @since 2.4.0
     */
    public long getSize()
    {
        try {
            this.indexLock.readLock().lock();

            File indexFile = getQueueIndexFile();
            if (!indexFile.exists() || indexFile.length() == 0) {
                return 0;
            }

            long offset = readHeadOffset();
            if (offset >= indexFile.length()) {
                return 0;
            }

            long count = 0;

            try (RandomAccessFile raf = new RandomAccessFile(indexFile, FILE_READ_ACCESS)) {
                raf.seek(offset);

                String line;
                while ((line = raf.readLine()) != null) {
                    if (!line.isEmpty()) {
                        count++;
                    }
                }
            }

            return count;
        } catch (Exception e) {
            this.logger.error("Failed to compute the sender message queue size", e);

            // Make it clear that the size could not be computed
            return -1;
        } finally {
            this.indexLock.readLock().unlock();
        }
    }
}
