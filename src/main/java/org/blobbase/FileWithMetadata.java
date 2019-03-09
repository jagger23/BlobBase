/*
 * Copyright (c) 2018 Tarokun LLC. All rights reserved.
 *
 * This file is part of BlobBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.blobbase;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A thin wrapper on top of the java File class to allow the setting of file attributes.
 * 
 */
public class FileWithMetadata extends File implements Serializable
{
    final private static long serialVersionUID = 1L;
        
    private Map<String,Object> map = new HashMap<String,Object>();
    
    public FileWithMetadata(File file)
    {
        super(file.toURI());
    }

    /**
     * @return the map
     */
    public Map<String,Object> getMap()
    {
        return map;
    }

    /**
     * @param map the map to set
     */
    public void setMap(Map<String,Object> map)
    {
        this.map = map;
    }
    
}
