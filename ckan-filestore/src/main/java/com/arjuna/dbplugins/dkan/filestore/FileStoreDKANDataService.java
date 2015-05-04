/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.dkan.filestore;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataService;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.PostConfig;
import com.arjuna.databroker.data.jee.annotation.PostCreated;
import com.arjuna.databroker.data.jee.annotation.PostRecovery;
import com.arjuna.databroker.data.jee.annotation.PreConfig;
import com.arjuna.databroker.data.jee.annotation.PreDelete;

public class FileStoreDKANDataService implements DataService
{
    private static final Logger logger = Logger.getLogger(FileStoreDKANDataService.class.getName());

    public static final String DKANROOTURL_PROPERTYNAME = "DKAN Root URL";
    public static final String PACKAGEID_PROPERTYNAME   = "Package Id";
    public static final String APIKEY_PROPERTYNAME      = "API Key";

    public FileStoreDKANDataService()
    {
        logger.log(Level.FINE, "FileStoreDKANDataService");
    }

    public FileStoreDKANDataService(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "FileStoreDKANDataService: " + name + ", " + properties);

        _name       = name;
        _properties = properties;
    }

    @Override
    public DataFlow getDataFlow()
    {
        return _dataFlow;
    }

    @Override
    public void setDataFlow(DataFlow dataFlow)
    {
        _dataFlow = dataFlow;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void setProperties(Map<String, String> properties)
    {
        _properties = properties;
    }

    @PostConfig
    @PostCreated
    @PostRecovery
    public void setup()
    {
        _dkanRootURL = _properties.get(DKANROOTURL_PROPERTYNAME);
        _packageId   = _properties.get(PACKAGEID_PROPERTYNAME);
        _apiKey      = _properties.get(APIKEY_PROPERTYNAME);
    }

    @PreConfig
    @PreDelete
    public void teardown()
    {
    }

    public void consumeString(String data)
    {
        logger.log(Level.FINE, "FileStoreDKANDataService.consume");

        try
        {
            URL uploadURL  = new URL(_dkanRootURL + "/api/action/resource_create");

            String boundaryText = UUID.randomUUID().toString();
            
            HttpURLConnection resourceCreateConnection = (HttpURLConnection) uploadURL.openConnection();
            resourceCreateConnection.setDoOutput(true);
            resourceCreateConnection.setDoInput(true);
            resourceCreateConnection.setInstanceFollowRedirects(false);
            resourceCreateConnection.setRequestMethod("POST");
            resourceCreateConnection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundaryText);
            resourceCreateConnection.setRequestProperty("Authorization", _apiKey);
            resourceCreateConnection.setUseCaches(false);
            
            try
            {
                OutputStream outputStream = resourceCreateConnection.getOutputStream();
            
                outputFormDataPart(outputStream, "package_id", null, _packageId.getBytes(), "form-data", boundaryText, true);
                outputFormDataPart(outputStream, "upload", "upload", data.getBytes(), "application/octet-stream", boundaryText, false);
                outputEndBoundary(outputStream, boundaryText);

                outputStream.close();
            }
            catch (IOException ioException)
            {
                logger.log(Level.WARNING, "Problems writing to dkan filestore api" + ioException);
            }

            if (resourceCreateConnection.getResponseCode() != 200)
                logger.log(Level.WARNING, "Problems with dkan filestore api invoke: status  = " + resourceCreateConnection.getResponseMessage());
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with dkan filestore api invoke", throwable);
        }
    }

    public void consumeBytes(byte[] data)
    {
        logger.log(Level.FINE, "FileStoredKANDataService.consume");

        try
        {
            URL uploadURL  = new URL(_dkanRootURL + "/api/action/resource_create");

            String boundaryText = UUID.randomUUID().toString();
            
            HttpURLConnection resourceCreateConnection = (HttpURLConnection) uploadURL.openConnection();
            resourceCreateConnection.setDoOutput(true);
            resourceCreateConnection.setDoInput(true);
            resourceCreateConnection.setInstanceFollowRedirects(false);
            resourceCreateConnection.setRequestMethod("POST");
            resourceCreateConnection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundaryText);
            resourceCreateConnection.setRequestProperty("Authorization", _apiKey);
            resourceCreateConnection.setUseCaches(false);
            
            try
            {
                OutputStream outputStream = resourceCreateConnection.getOutputStream();
            
                outputFormDataPart(outputStream, "package_id", null, _packageId.getBytes(), "form-data", boundaryText, true);
                outputFormDataPart(outputStream, "upload", "upload", data, "application/octet-stream", boundaryText, false);
                outputEndBoundary(outputStream, boundaryText);

                outputStream.close();
            }
            catch (IOException ioException)
            {
                logger.log(Level.WARNING, "Problems writing to dkan filestore api" + ioException);
            }

            if (resourceCreateConnection.getResponseCode() != 200)
                logger.log(Level.WARNING, "Problems with dkan filestore api invoke: status  = " + resourceCreateConnection.getResponseMessage());
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with dkan filestore api invoke", throwable);
        }
    }

    private void outputFormDataPart(OutputStream outputStream, String name, String filename, byte[] value, String contentType, String boundaryText, boolean firstOutput)
        throws IOException
    {
        if (firstOutput)
            outputStream.write("--".getBytes());
        else
            outputStream.write("\r\n--".getBytes());
        outputStream.write(boundaryText.getBytes());
        outputStream.write("\r\n".getBytes());
        outputStream.write("Content-Disposition: form-data; name=\"".getBytes());
        outputStream.write(name.getBytes());
        if (filename != null)
        {
            outputStream.write("\"; filename=\"".getBytes());
            outputStream.write(filename.getBytes());            
        }
        outputStream.write("\"\r\n".getBytes());
        if (! "form-data".equals(contentType))
        {
            outputStream.write("Content-Type: ".getBytes());
            outputStream.write(contentType.getBytes());            
            outputStream.write("\r\n".getBytes());
        }
        outputStream.write("\r\n".getBytes());
        outputStream.write(value);
        outputStream.flush();
    }

    private void outputEndBoundary(OutputStream outputStream, String boundaryText)
        throws IOException
    {
        outputStream.write("\r\n--".getBytes());
        outputStream.write(boundaryText.getBytes());
        outputStream.write("--\r\n".getBytes());
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        return dataConsumerDataClasses;
    }

    @Override
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        return null;
    }

    @Override
    public Collection<Class<?>> getDataConsumerDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        dataConsumerDataClasses.add(String.class);
        dataConsumerDataClasses.add(byte[].class);

        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (dataClass == String.class)
            return (DataConsumer<T>) _dataConsumerString;
        else if (dataClass == byte[].class)
            return (DataConsumer<T>) _dataConsumerBytes;
        else
            return null;
    }

    private String _dkanRootURL;
    private String _packageId;
    private String _apiKey;

    private DataFlow             _dataFlow;
    private String               _name;
    private Map<String, String>  _properties;
    @DataConsumerInjection(methodName="consumeString")
    private DataConsumer<String> _dataConsumerString;
    @DataConsumerInjection(methodName="consumeBytes")
    private DataConsumer<byte[]> _dataConsumerBytes;
}
