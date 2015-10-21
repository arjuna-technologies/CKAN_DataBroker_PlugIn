/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.ckan;

import java.util.Collections;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import com.arjuna.databroker.data.DataFlowNodeFactory;
import com.arjuna.databroker.data.DataFlowNodeFactoryInventory;
import com.arjuna.dbplugins.ckan.filestore.AppendFileStoreCKANDataFlowNodeFactory;
import com.arjuna.dbplugins.ckan.filestore.UpdateFileStoreCKANDataFlowNodeFactory;

@Startup
@Singleton
public class CKANDataFlowNodeFactoriesSetup
{
    @PostConstruct
    public void setup()
    {
        DataFlowNodeFactory appendFileStoreCKANDataFlowNodeFactory = new AppendFileStoreCKANDataFlowNodeFactory("Append File Store CKAN Data Flow Node Factories", Collections.<String, String>emptyMap());
        DataFlowNodeFactory updateFileStoreCKANDataFlowNodeFactory = new UpdateFileStoreCKANDataFlowNodeFactory("Update File Store CKAN Data Flow Node Factories", Collections.<String, String>emptyMap());

        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(appendFileStoreCKANDataFlowNodeFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(updateFileStoreCKANDataFlowNodeFactory);
    }

    @PreDestroy
    public void cleanup()
    {
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Append File Store CKAN Data Flow Node Factories");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Update File Store CKAN Data Flow Node Factories");
    }

    @EJB(lookup="java:global/databroker/data-core-jee/DataFlowNodeFactoryInventory")
    private DataFlowNodeFactoryInventory _dataFlowNodeFactoryInventory;
}
