/*
 * Copyright (c) 2013-2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.ckan.filestore.dataflownodes;

import java.io.Serializable;

public class ResourceUpdaterResponceDTO implements Serializable
{
	private static final long serialVersionUID = -5571259199998367286L;

	public ResourceUpdaterResponceDTO()
    {
    }

    public ResourceUpdaterResponceDTO(String stuff)
    {
        _stuff = stuff;
    }

    public String getStuff()
    {
        return _stuff;
    }

    public void setStuff(String stuff)
    {
        _stuff = stuff;
    }

    private String _stuff;
}