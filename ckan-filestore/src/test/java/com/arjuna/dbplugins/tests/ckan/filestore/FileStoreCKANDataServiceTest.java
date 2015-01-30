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
import com.arjuna.dbplugins.ckan.filestore.FileStoreCKANDataService;
import com.arjuna.dbutilities.testsupport.dataflownodes.dummy.DummyDataSource;
import com.arjuna.dbutilities.testsupport.dataflownodes.lifecycle.TestJEEDataFlowNodeLifeCycleControl;

public class FileStoreCKANDataServiceTest
{
    private static final Logger logger = Logger.getLogger(FileStoreCKANDataServiceTest.class.getName());

    @Test
    public void createResource()
    {
        try
        {
            CKANAPIProperties ckanAPIProperties = new CKANAPIProperties("ckanapi.properties");

            if (! ckanAPIProperties.isLoaded())
            {
                logger.log(Level.INFO, "SKIPPING TEST 'createResource', no propertiles file");
                return;
            }

            DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

            String              name       = "FileStoreCKANDataService";
            Map<String, String> properties = new HashMap<String, String>();
            properties.put(FileStoreCKANDataService.CKANROOTURL_PROPERTYNAME, ckanAPIProperties.getCKANRootURL());
            properties.put(FileStoreCKANDataService.PACKAGEID_PROPERTYNAME, ckanAPIProperties.getPackageId());
            properties.put(FileStoreCKANDataService.APIKEY_PROPERTYNAME, ckanAPIProperties.getAPIKey());

            DummyDataSource          dummyDataSource          = new DummyDataSource("Dummy Data Source", Collections.<String, String>emptyMap());
            FileStoreCKANDataService fileStoreCKANDataService = new FileStoreCKANDataService(name, properties);

            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSource, null);
            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), fileStoreCKANDataService, null);

            ((ObservableDataProvider<String>) dummyDataSource.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) fileStoreCKANDataService.getDataConsumer(String.class));

            dummyDataSource.sendData("Test Data, Test Text");
            
            dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSource);
            dataFlowNodeLifeCycleControl.removeDataFlowNode(fileStoreCKANDataService);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem in 'createResource'", throwable);
            fail("Problem in 'createResource': " + throwable);
        }
    }
}
