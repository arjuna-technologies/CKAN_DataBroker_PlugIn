/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.ckan.filestore.dataflownodes;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.MediaType;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.util.GenericType;
import org.jboss.resteasy.util.HttpResponseCodes;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataService;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;

public class FileStoreCKANDataService implements DataService
{
    private static final Logger logger = Logger.getLogger(FileStoreCKANDataService.class.getName());

    public static final String SERVICEROOTURL_PROPERTYNAME = "Service Root URL";
    public static final String RESOURCEID_PROPERTYNAME     = "Resource Id";
    public static final String APIKEY_PROPERTYNAME         = "API Key";

    public FileStoreCKANDataService(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "FileStoreCKANDataService: " + name + ", " + properties);

        _name       = name;
        _properties = properties;

        _serviceRootURL = properties.get(SERVICEROOTURL_PROPERTYNAME);
        _resourceId     = properties.get(RESOURCEID_PROPERTYNAME);
        _apiKey         = properties.get(APIKEY_PROPERTYNAME);
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

    public void consume(String data)
    {
        logger.log(Level.FINE, "FileStoreCKANDataService.consume");

        try
        {
            ClientRequest request = new ClientRequest(_serviceRootURL + "/api/3/action/resource_update");
            request.accept(MediaType.MULTIPART_FORM_DATA);
            request.header("Authorization", _apiKey);
            request.body("id", _resourceId);
            request.body("upload", data);

            ClientResponse<ResourceUpdaterResponceDTO> response = request.get(new GenericType<ResourceUpdaterResponceDTO>() {});

            if (response.getStatus() == HttpResponseCodes.SC_OK)
            {
            	ResourceUpdaterResponceDTO resourceUpdaterResponceDTO = response.getEntity();
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

    private String _serviceRootURL;
    private String _resourceId;
    private String _apiKey;

    private DataFlow               _dataFlow;
    private String                 _name;
    private Map<String, String>    _properties;
    @DataConsumerInjection(methodName="consume")
    private DataConsumer<String> _dataConsumer;
}
