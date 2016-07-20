/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.ckan.filestore;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@Deprecated
public class FileStoreCKANDataService extends AppendFileStoreCKANDataService
{
    private static final Logger logger = Logger.getLogger(FileStoreCKANDataService.class.getName());

    public FileStoreCKANDataService()
    {
        super();

        logger.log(Level.FINE, "FileStoreCKANDataService");
    }

    public FileStoreCKANDataService(String name, Map<String, String> properties)
    {
        super(name, properties);

        logger.log(Level.FINE, "FileStoreCKANDataService: " + name + ", " + properties);
    }
}
