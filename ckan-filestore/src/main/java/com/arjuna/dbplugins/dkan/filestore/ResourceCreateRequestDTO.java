/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.dkan.filestore;

import java.io.Serializable;

public class ResourceCreateRequestDTO implements Serializable
{
    private static final long serialVersionUID = -5571259199998367286L;

    public ResourceCreateRequestDTO()
    {
    }

    public ResourceCreateRequestDTO(Boolean success)
    {
        _success = success;
    }

    public Boolean getSuccess()
    {
        return _success;
    }

    public void setSuccess(Boolean success)
    {
        _success = success;
    }

    private Boolean _success;
}
