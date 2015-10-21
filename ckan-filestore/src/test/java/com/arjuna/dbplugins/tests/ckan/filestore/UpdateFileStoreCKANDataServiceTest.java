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
import com.arjuna.dbplugins.ckan.filestore.UpdateFileStoreCKANDataService;
import com.arjuna.dbutils.testsupport.dataflownodes.dummy.DummyDataSource;
import com.arjuna.dbutils.testsupport.dataflownodes.lifecycle.TestJEEDataFlowNodeLifeCycleControl;

public class UpdateFileStoreCKANDataServiceTest
{
    private static final Logger logger = Logger.getLogger(UpdateFileStoreCKANDataServiceTest.class.getName());

    @Test
    public void createResourceAsString()
    {
        try
        {
            CKANAPIProperties ckanAPIProperties = new CKANAPIProperties("ckanapi.properties");

            if (! ckanAPIProperties.isLoaded())
            {
                logger.log(Level.INFO, "SKIPPING TEST 'UpdateFileStoreCKANDataServiceTest.createResourceAsString', no propertiles file");
                return;
            }

            DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

            String              name       = "UpdateFileStoreCKANDataService";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(UpdateFileStoreCKANDataService.CKANROOTURL_PROPERTYNAME, ckanAPIProperties.getCKANRootURL());
            properties.put(UpdateFileStoreCKANDataService.PACKAGEID_PROPERTYNAME, ckanAPIProperties.getPackageId());
            properties.put(UpdateFileStoreCKANDataService.APIKEY_PROPERTYNAME, ckanAPIProperties.getAPIKey());

            DummyDataSource                dummyDataSource                = new DummyDataSource("Dummy Data Source", Collections.<String, String>emptyMap());
            UpdateFileStoreCKANDataService updateFileStoreCKANDataService = new UpdateFileStoreCKANDataService(name, properties);

            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSource, null);
            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), updateFileStoreCKANDataService, null);

            ((ObservableDataProvider<String>) dummyDataSource.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) updateFileStoreCKANDataService.getDataConsumer(String.class));

            dummyDataSource.sendData("Test Data, Test Text, Part 1");
            dummyDataSource.sendData("Test Data, Test Text, Part 2");

            dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSource);
            dataFlowNodeLifeCycleControl.removeDataFlowNode(updateFileStoreCKANDataService);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'UpdateFileStoreCKANDataServiceTest.createResourceAsString'", throwable);
            fail("Problem in 'UpdateFileStoreCKANDataServiceTest.createResourceAsString': " + throwable);
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
                logger.log(Level.INFO, "SKIPPING TEST 'UpdateFileStoreCKANDataServiceTest.createResourceAsBytes', no propertiles file");
                return;
            }

            DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

            String              name       = "UpdateFileStoreCKANDataService";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(UpdateFileStoreCKANDataService.CKANROOTURL_PROPERTYNAME, ckanAPIProperties.getCKANRootURL());
            properties.put(UpdateFileStoreCKANDataService.PACKAGEID_PROPERTYNAME, ckanAPIProperties.getPackageId());
            properties.put(UpdateFileStoreCKANDataService.APIKEY_PROPERTYNAME, ckanAPIProperties.getAPIKey());

            DummyDataSource                dummyDataSource                = new DummyDataSource("Dummy Data Source", Collections.<String, String>emptyMap());
            UpdateFileStoreCKANDataService updateFileStoreCKANDataService = new UpdateFileStoreCKANDataService(name, properties);

            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSource, null);
            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), updateFileStoreCKANDataService, null);

            ((ObservableDataProvider<byte[]>) dummyDataSource.getDataProvider(byte[].class)).addDataConsumer((ObserverDataConsumer<byte[]>) updateFileStoreCKANDataService.getDataConsumer(byte[].class));

            dummyDataSource.sendData("Test Data, Test Text, Part 1".getBytes());
            dummyDataSource.sendData("Test Data, Test Text, Part 2".getBytes());

            dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSource);
            dataFlowNodeLifeCycleControl.removeDataFlowNode(updateFileStoreCKANDataService);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'UpdateFileStoreCKANDataServiceTest.createResourceAsBytes'", throwable);
            fail("Problem in 'UpdateFileStoreCKANDataServiceTest.createResourceAsBytes': " + throwable);
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
                logger.log(Level.INFO, "SKIPPING TEST 'UpdateFileStoreCKANDataServiceTest.createResourceMap', no propertiles file");
                return;
            }

            DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

            String              name       = "UpdateFileStoreCKANDataService";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(UpdateFileStoreCKANDataService.CKANROOTURL_PROPERTYNAME, ckanAPIProperties.getCKANRootURL());
            properties.put(UpdateFileStoreCKANDataService.PACKAGEID_PROPERTYNAME, ckanAPIProperties.getPackageId());
            properties.put(UpdateFileStoreCKANDataService.APIKEY_PROPERTYNAME, ckanAPIProperties.getAPIKey());

            DummyDataSource                dummyDataSource                = new DummyDataSource("Dummy Data Source", Collections.<String, String>emptyMap());
            UpdateFileStoreCKANDataService updateFileStoreCKANDataService = new UpdateFileStoreCKANDataService(name, properties);

            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSource, null);
            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), updateFileStoreCKANDataService, null);

            ((ObservableDataProvider<Map>) dummyDataSource.getDataProvider(Map.class)).addDataConsumer((ObserverDataConsumer<Map>) updateFileStoreCKANDataService.getDataConsumer(Map.class));

            Map<String, Object> dataMap = new HashMap<String, Object>();
            dataMap.put("data", "Test Data, Test Text, Part 1".getBytes());
            dataMap.put("filename", "filename");
            dataMap.put("resourcename", "resourcename");
            dataMap.put("resourceformat", "TEXT");
            dataMap.put("resourcedescription", "A description");
            dummyDataSource.sendData(dataMap);
            dataMap.put("data", "Test Data, Test Text, Part 2".getBytes());
            dummyDataSource.sendData(dataMap);

            dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSource);
            dataFlowNodeLifeCycleControl.removeDataFlowNode(updateFileStoreCKANDataService);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'UpdateFileStoreCKANDataServiceTest.createResourceMap'", throwable);
            fail("Problem in 'UpdateFileStoreCKANDataServiceTest.createResourceMap': " + throwable);
        }
    }
}
