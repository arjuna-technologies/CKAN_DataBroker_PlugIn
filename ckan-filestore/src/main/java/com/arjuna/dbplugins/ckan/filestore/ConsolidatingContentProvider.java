/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.ckan.filestore;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.eclipse.jetty.client.util.AbstractTypedContentProvider;

/*
 * A Jetty ContentProvider that consolidates data, so avoiding 'chunking'
 */

public class ConsolidatingContentProvider extends AbstractTypedContentProvider
{
    public ConsolidatingContentProvider(Typed contentProvider)
    {
        super(contentProvider.getContentType());

        Iterator<ByteBuffer> iterator;

        iterator = contentProvider.iterator();

        _length = 0;
        while (iterator.hasNext())
            _length += iterator.next().remaining();

        _byteBuffer = ByteBuffer.allocate((int) getLength());
        iterator = contentProvider.iterator();
        while (iterator.hasNext())
            _byteBuffer.put(iterator.next());
        _byteBuffer.rewind();
    }

    @Override
    public long getLength()
    {
        return _length;
    }

    @Override
    public Iterator<ByteBuffer> iterator()
    {
        return new Iterator<ByteBuffer>()
        {
            @Override
            public boolean hasNext()
            {
                return _currentByteBuffer != null;
            }

            @Override
            public ByteBuffer next()
            {
                if (_currentByteBuffer == null)
                    throw new NoSuchElementException();

                ByteBuffer resultByteBuffer = _currentByteBuffer;
                _currentByteBuffer = null;

                return resultByteBuffer;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            private ByteBuffer _currentByteBuffer = _byteBuffer;
        };
    }

    private long       _length;
    private ByteBuffer _byteBuffer;
}
