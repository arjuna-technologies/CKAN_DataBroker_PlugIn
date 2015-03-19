/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.ckan.filestore;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
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

public class FileStoreCKANDataService implements DataService
{
    private static final Logger logger = Logger.getLogger(FileStoreCKANDataService.class.getName());

    public static final String CKANROOTURL_PROPERTYNAME = "CKAN Root URL";
    public static final String PACKAGEID_PROPERTYNAME   = "Package Id";
    public static final String APIKEY_PROPERTYNAME      = "API Key";

    public FileStoreCKANDataService()
    {
        logger.log(Level.FINE, "FileStoreCKANDataService");
    }

    public FileStoreCKANDataService(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "FileStoreCKANDataService: " + name + ", " + properties);

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
        _ckanRootURL = _properties.get(CKANROOTURL_PROPERTYNAME);
        _packageId   = _properties.get(PACKAGEID_PROPERTYNAME);
        _apiKey      = _properties.get(APIKEY_PROPERTYNAME);

        try
        {
            _httpClient = new HttpClient();
            _httpClient.start();
        }
        catch (Throwable throwable)
        {
            logger.log(Level.FINER, "Problem in setup of httpClient", throwable);
        }
    }

    @PreConfig
    @PreDelete
    public void teardown()
    {
        try
        {
            _httpClient.stop();
            _httpClient = null;
        }
        catch (Throwable throwable)
        {
            logger.log(Level.FINER, "Problem in teardown of httpClient", throwable);
        }
    }

    public void consumeString(String data)
    {
        logger.log(Level.FINE, "FileStoreCKANDataService.consume");

        try
        {
            MultipartFormDataContentProvider multipartFormDataContentProvider = new MultipartFormDataContentProvider(UUID.randomUUID().toString());
            multipartFormDataContentProvider.getParts().add(new MultipartFormDataContentProvider.Part("upload", "upload", new BytesContentProvider("application/octet-stream", data.getBytes())));
            multipartFormDataContentProvider.getParts().add(new MultipartFormDataContentProvider.Part("package_id", new StringContentProvider("form-data", _packageId, Charset.defaultCharset())));

            Request request = _httpClient.newRequest(_ckanRootURL + "/api/action/resource_create");
            request.method(HttpMethod.POST);
            request.header(HttpHeader.AUTHORIZATION, _apiKey);
            request.content(multipartFormDataContentProvider);

            ContentResponse response = request.send();

            if (response.getStatus() != HttpStatus.OK_200)
            {
                logger.log(Level.WARNING, "Problems with ckan filestore api invoke: status  = " + response.getStatus());
                logger.log(Level.WARNING, "                                         content = [" + response.getContentAsString() + "]");
            }
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with ckan filestore api invoke", throwable);
        }
    }

    public void consumeBytes(byte[] data)
    {
        logger.log(Level.FINE, "FileStoreCKANDataService.consume");

        try
        {
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with ckan filestore api invoke", throwable);
        }
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

    private String _ckanRootURL;
    private String _packageId;
    private String _apiKey;

    private HttpClient _httpClient;

    private DataFlow             _dataFlow;
    private String               _name;
    private Map<String, String>  _properties;
    @DataConsumerInjection(methodName="consumeString")
    private DataConsumer<String> _dataConsumerString;
    @DataConsumerInjection(methodName="consumeBytes")
    private DataConsumer<byte[]> _dataConsumerBytes;
}
