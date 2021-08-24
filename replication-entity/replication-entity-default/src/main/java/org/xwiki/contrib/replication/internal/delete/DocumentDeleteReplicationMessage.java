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
package org.xwiki.contrib.replication.internal.delete;

import java.io.IOException;
import java.io.OutputStream;

import org.xwiki.component.annotation.Component;
import org.xwiki.contrib.replication.internal.AbstractDocumentReplicationMessage;

/**
 * @version $Id$
 * @since 0.3
 */
@Component(roles = DocumentDeleteReplicationMessage.class)
public class DocumentDeleteReplicationMessage extends AbstractDocumentReplicationMessage
{
    /**
     * The message type for these messages.
     */
    public static final String TYPE = TYPE_PREFIX + "delete";

    /**
     * The prefix in front of all entity metadata properties.
     */
    public static final String METADATA_PREFIX = TYPE.toUpperCase() + '_';

    @Override
    public String getType()
    {
        return TYPE;
    }

    @Override
    public void write(OutputStream stream) throws IOException
    {
        // No content associated with this event
    }
}