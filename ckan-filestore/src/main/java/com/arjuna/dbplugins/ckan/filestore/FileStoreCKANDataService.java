/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.ckan.filestore;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.MediaType;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataOutput;
import org.jboss.resteasy.util.GenericType;
import org.jboss.resteasy.util.HttpResponseCodes;
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
            ClientRequest request = new ClientRequest(_ckanRootURL + "/api/action/resource_create");

            String name = UUID.randomUUID().toString();
            MultipartFormDataOutput upload = new MultipartFormDataOutput();
            upload.addFormData("package_id", _packageId, MediaType.APPLICATION_OCTET_STREAM_TYPE);
            upload.addFormData("name", name, MediaType.APPLICATION_OCTET_STREAM_TYPE);

            upload.addFormData("url", "http://example.org/" + name, MediaType.APPLICATION_OCTET_STREAM_TYPE);

            upload.addPart(data, MediaType.APPLICATION_OCTET_STREAM_TYPE);

            request.header("Authorization", _apiKey);
            request.body(MediaType.MULTIPART_FORM_DATA_TYPE, upload);

            ClientResponse<ResourceUpdaterResponceDTO> response = request.post(new GenericType<ResourceUpdaterResponceDTO>() {});

            if (response.getStatus() == HttpResponseCodes.SC_OK)
            {
                String error = response.getEntity(String.class);
                logger.log(Level.FINE, "Success: [" + error + "]");
            }
            else if (response.getStatus() != HttpResponseCodes.SC_CONFLICT)
            {
                String error = response.getEntity(String.class);
                logger.log(Level.WARNING, "Error: [" + error + "]");
            }
            else
                logger.log(Level.WARNING, "DataBrokerClient.getDataBrokerSummaries: status = " + response.getStatus());
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
    @SuppressWarnings("unchecked")
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
