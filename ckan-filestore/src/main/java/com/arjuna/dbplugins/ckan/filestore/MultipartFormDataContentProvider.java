/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.ckan.filestore;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.util.AbstractTypedContentProvider;

/*
 * A Jetty ContentProvider for Multipart Form Data
 */

public class MultipartFormDataContentProvider extends AbstractTypedContentProvider
{
    private static final Logger logger = Logger.getLogger(MultipartFormDataContentProvider.class.getName());

    public MultipartFormDataContentProvider()
    {
        super("multipart/form-data");

        _contentProviders = new LinkedList<ContentProvider>();
    }

    @Override
    public long getLength()
    {
        return -1;
    }

    @Override
    public Iterator<ByteBuffer> iterator()
    {
        return new Iterator<ByteBuffer>()
        {
            private Iterator<ContentProvider> _contentProviderIterator = _contentProviders.iterator();
            private Iterator<ByteBuffer>      _iterator                = null;

            @Override
            public boolean hasNext()
            {
                boolean hasNext = (_contentProviderIterator.hasNext() || ((_iterator != null) && _iterator.hasNext()));

                logger.log(Level.INFO, "hasNext: " + hasNext);

                return hasNext;
            }

            @Override
            public ByteBuffer next()
            {
                logger.log(Level.INFO, "next: " + _iterator + ", " + _contentProviderIterator);

                if ((_iterator == null) || (! _iterator.hasNext()))
                {
                    if (_contentProviderIterator.hasNext())
                        _iterator = _contentProviderIterator.next().iterator();
                    else
                        throw new NoSuchElementException();
                }

                return _iterator.next();
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public List<ContentProvider> getParts()
    {
        logger.log(Level.INFO, "getParts: " + _contentProviders.size());

        return _contentProviders;
    }

    private List<ContentProvider> _contentProviders;
}
