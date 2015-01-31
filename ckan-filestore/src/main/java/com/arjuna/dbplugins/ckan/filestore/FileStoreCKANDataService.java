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
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataService;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.PostConfig;
import com.arjuna.databroker.data.jee.annotation.PostCreated;
import com.arjuna.databroker.data.jee.annotation.PostRecovery;

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
    public void config()
    {
        _ckanRootURL = _properties.get(CKANROOTURL_PROPERTYNAME);
        _packageId   = _properties.get(PACKAGEID_PROPERTYNAME);
        _apiKey      = _properties.get(APIKEY_PROPERTYNAME);
    }

    public void consume(String data)
    {
        logger.log(Level.FINE, "FileStoreCKANDataService.consume");

        try
        {
            String name = UUID.randomUUID().toString();
            StringBody packageIdBody = new StringBody(_packageId, ContentType.MULTIPART_FORM_DATA);
            StringBody nameBody      = new StringBody(name, ContentType.MULTIPART_FORM_DATA);

            MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
            multipartEntityBuilder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
            multipartEntityBuilder.addPart("package_id", packageIdBody);
            multipartEntityBuilder.addPart("name", nameBody);
            multipartEntityBuilder.addBinaryBody("upload", data.getBytes(Charset.forName("UTF-8")), ContentType.DEFAULT_BINARY, name);

            HttpClient httpClient = HttpClientBuilder.create().build();

            HttpPost request = new HttpPost(_ckanRootURL + "/api/action/resource_create");
            request.addHeader("Authorization", _apiKey);
            request.setEntity(multipartEntityBuilder.build());

            HttpResponse response = httpClient.execute(request);

            if ((response.getStatusLine() != null) && (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK))
            {
                logger.log(Level.WARNING, "Problems with ckan filestore api invoke: status = " + response.getStatusLine());
                if (logger.isLoggable(Level.FINER))
                {
                    String responseMessage = EntityUtils.toString(response.getEntity());
                    logger.log(Level.FINER, "Reponse message: [" + responseMessage + "]");
                }
            }
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

        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (dataClass == String.class)
            return (DataConsumer<T>) _dataConsumer;
        else
            return null;
    }

    private String _ckanRootURL;
    private String _packageId;
    private String _apiKey;

    private DataFlow             _dataFlow;
    private String               _name;
    private Map<String, String>  _properties;
    @DataConsumerInjection(methodName="consume")
    private DataConsumer<String> _dataConsumer;
}
