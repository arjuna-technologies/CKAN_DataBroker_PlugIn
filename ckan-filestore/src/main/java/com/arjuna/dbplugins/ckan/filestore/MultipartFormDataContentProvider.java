/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.ckan.filestore;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import org.eclipse.jetty.client.util.AbstractTypedContentProvider;

/*
 * A Jetty ContentProvider for Multipart Form Data
 */

public class MultipartFormDataContentProvider extends AbstractTypedContentProvider
{
    public MultipartFormDataContentProvider(String boundaryText)
    {
        super("multipart/form-data; boundary=" + boundaryText);

        _boundaryText = boundaryText;
        _parts        = new LinkedList<Part>();
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
            @Override
            public boolean hasNext()
            {
                boolean hasNext = _partIterator != null;

                return hasNext;
            }

            @Override
            public ByteBuffer next()
            {
                if ( _partIterator == null)
                    throw new NoSuchElementException();

                if ((_byteBufferIterator != null) && (! _byteBufferIterator.hasNext()))
                {
                    _part               = null;
                    _byteBufferIterator = null;
                }

                if (_part == null)
                {
                    if (_partIterator.hasNext())
                        _part = _partIterator.next();
                    else
                    {
                        _partIterator = null;

                        StringBuffer seperatorText = new StringBuffer();
                        seperatorText.append("\r\n--");
                        seperatorText.append(_boundaryText);
                        seperatorText.append("--\r\n");

                        return ByteBuffer.wrap(seperatorText.toString().getBytes());
                    }
                }

                if (_byteBufferIterator == null)
                {
                    _byteBufferIterator = _part.getContentProvider().iterator();

                    StringBuffer seperatorText = new StringBuffer();
                    seperatorText.append("\r\n--");
                    seperatorText.append(_boundaryText);
                    seperatorText.append("\r\nContent-Disposition: form-data; name=\"");
                    seperatorText.append(_part.getName());
                    if (_part.getFilename() != null)
                    {
                        seperatorText.append("\"; filename=\"");
                        seperatorText.append(_part.getFilename());
                    }
                    seperatorText.append("\"\r\n");
                    if (! "form-data".equals(_part.getContentProvider().getContentType()))
                    {
                        seperatorText.append("Content-Type: ");
                        seperatorText.append(_part.getContentProvider().getContentType());
                        seperatorText.append("\r\n");
                    }
                    seperatorText.append("\r\n");

                    return ByteBuffer.wrap(seperatorText.toString().getBytes());
                }
                else
                    return _byteBufferIterator.next();
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            private Iterator<Part>       _partIterator       = _parts.iterator();
            private Part                 _part               = null;
            private Iterator<ByteBuffer> _byteBufferIterator = null;
        };
    }

    public List<Part> getParts()
    {
        return _parts;
    }

    public static class Part
    {
        public Part(String name, Typed contentProvider)
        {
            _name            = name;
            _filename        = null;
            _contentProvider = contentProvider;
        }

        public Part(String name, String filename, Typed contentProvider)
        {
            _name            = name;
            _filename        = filename;
            _contentProvider = contentProvider;
        }

        public String getName()
        {
            return _name;
        }

        public String getFilename()
        {
            return _filename;
        }

        public Typed getContentProvider()
        {
            return _contentProvider;
        }

        private String _name;
        private String _filename;
        private Typed  _contentProvider;
    }

    private String     _boundaryText;
    private List<Part> _parts;
}
