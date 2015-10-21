/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.tests.ckan.filestore;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;
import com.arjuna.databroker.data.connector.ObservableDataProvider;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.databroker.data.core.DataFlowNodeLifeCycleControl;
import com.arjuna.dbplugins.ckan.filestore.AppendFileStoreCKANDataService;
import com.arjuna.dbutils.testsupport.dataflownodes.dummy.DummyDataSource;
import com.arjuna.dbutils.testsupport.dataflownodes.lifecycle.TestJEEDataFlowNodeLifeCycleControl;

public class AppendFileStoreCKANDataServiceTest
{
    private static final Logger logger = Logger.getLogger(AppendFileStoreCKANDataServiceTest.class.getName());

    @Test
    public void createResourceAsString()
    {
        try
        {
            CKANAPIProperties ckanAPIProperties = new CKANAPIProperties("ckanapi.properties");

            if (! ckanAPIProperties.isLoaded())
            {
                logger.log(Level.INFO, "SKIPPING TEST 'AppendFileStoreCKANDataServiceTest.createResourceAsString', no propertiles file");
                return;
            }

            DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

            String              name       = "AppendFileStoreCKANDataService";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(AppendFileStoreCKANDataService.CKANROOTURL_PROPERTYNAME, ckanAPIProperties.getCKANRootURL());
            properties.put(AppendFileStoreCKANDataService.PACKAGEID_PROPERTYNAME, ckanAPIProperties.getPackageId());
            properties.put(AppendFileStoreCKANDataService.APIKEY_PROPERTYNAME, ckanAPIProperties.getAPIKey());

            DummyDataSource                dummyDataSource                = new DummyDataSource("Dummy Data Source", Collections.<String, String>emptyMap());
            AppendFileStoreCKANDataService appendFileStoreCKANDataService = new AppendFileStoreCKANDataService(name, properties);

            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSource, null);
            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), appendFileStoreCKANDataService, null);

            ((ObservableDataProvider<String>) dummyDataSource.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) appendFileStoreCKANDataService.getDataConsumer(String.class));

            dummyDataSource.sendData("Test Data, Test Text");

            dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSource);
            dataFlowNodeLifeCycleControl.removeDataFlowNode(appendFileStoreCKANDataService);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'AppendFileStoreCKANDataServiceTest.createResourceAsString'", throwable);
            fail("Problem in 'AppendFileStoreCKANDataServiceTest.createResourceAsString': " + throwable);
        }
    }

    @Test
    public void createResourceAsBytes()
    {
        try
        {
            CKANAPIProperties ckanAPIProperties = new CKANAPIProperties("ckanapi.properties");

            if (! ckanAPIProperties.isLoaded())
            {
                logger.log(Level.INFO, "SKIPPING TEST 'AppendFileStoreCKANDataServiceTest.createResourceAsBytes', no propertiles file");
                return;
            }

            DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

            String              name       = "AppendFileStoreCKANDataService";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(AppendFileStoreCKANDataService.CKANROOTURL_PROPERTYNAME, ckanAPIProperties.getCKANRootURL());
            properties.put(AppendFileStoreCKANDataService.PACKAGEID_PROPERTYNAME, ckanAPIProperties.getPackageId());
            properties.put(AppendFileStoreCKANDataService.APIKEY_PROPERTYNAME, ckanAPIProperties.getAPIKey());

            DummyDataSource                dummyDataSource                = new DummyDataSource("Dummy Data Source", Collections.<String, String>emptyMap());
            AppendFileStoreCKANDataService appendFileStoreCKANDataService = new AppendFileStoreCKANDataService(name, properties);

            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSource, null);
            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), appendFileStoreCKANDataService, null);

            ((ObservableDataProvider<byte[]>) dummyDataSource.getDataProvider(byte[].class)).addDataConsumer((ObserverDataConsumer<byte[]>) appendFileStoreCKANDataService.getDataConsumer(byte[].class));

            dummyDataSource.sendData("Test Data, Test Text".getBytes());

            dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSource);
            dataFlowNodeLifeCycleControl.removeDataFlowNode(appendFileStoreCKANDataService);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'AppendFileStoreCKANDataServiceTest.createResourceAsBytes'", throwable);
            fail("Problem in 'AppendFileStoreCKANDataServiceTest.createResourceAsBytes': " + throwable);
        }
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void createResourceMap()
    {
        try
        {
            CKANAPIProperties ckanAPIProperties = new CKANAPIProperties("ckanapi.properties");

            if (! ckanAPIProperties.isLoaded())
            {
                logger.log(Level.INFO, "SKIPPING TEST 'AppendFileStoreCKANDataServiceTest.createResourceMap', no propertiles file");
                return;
            }

            DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

            String              name       = "AppendFileStoreCKANDataService";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(AppendFileStoreCKANDataService.CKANROOTURL_PROPERTYNAME, ckanAPIProperties.getCKANRootURL());
            properties.put(AppendFileStoreCKANDataService.PACKAGEID_PROPERTYNAME, ckanAPIProperties.getPackageId());
            properties.put(AppendFileStoreCKANDataService.APIKEY_PROPERTYNAME, ckanAPIProperties.getAPIKey());

            DummyDataSource                dummyDataSource                = new DummyDataSource("Dummy Data Source", Collections.<String, String>emptyMap());
            AppendFileStoreCKANDataService appendFileStoreCKANDataService = new AppendFileStoreCKANDataService(name, properties);

            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSource, null);
            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), appendFileStoreCKANDataService, null);

            ((ObservableDataProvider<Map>) dummyDataSource.getDataProvider(Map.class)).addDataConsumer((ObserverDataConsumer<Map>) appendFileStoreCKANDataService.getDataConsumer(Map.class));

            Map<String, Object> dataMap = new HashMap<String, Object>();
            dataMap.put("data", "Test Data, Test Text".getBytes());
            dataMap.put("filename", "filename");
            dataMap.put("resourcename", "resourcename");
            dataMap.put("resourceformat", "TEXT");
            dataMap.put("resourcedescription", "A description");
            dummyDataSource.sendData(dataMap);

            dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSource);
            dataFlowNodeLifeCycleControl.removeDataFlowNode(appendFileStoreCKANDataService);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'AppendFileStoreCKANDataServiceTest.createResourceMap'", throwable);
            fail("Problem in 'AppendFileStoreCKANDataServiceTest.createResourceMap': " + throwable);
        }
    }
}
