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
 */
package org.blobbase;

import java.io.File;
import java.io.Serializable;
import java.net.URI;

/**
 * A wrapper for the standard Java File class that overrides certain methods that would
 * might cause database problems if they we 'accidentally' invoked and are thus prohibited.
 * 
 */
public class BlobFile extends File implements Serializable
{
    final private static long serialVersionUID = 1L;

    /**
     * Creates a new BlobFile instance from a parent abstract pathname and a child
     * pathname string.
     *
     * @param parent
     * @param child
     */
    BlobFile(File parent, String child)
    {
        super(parent,child);
    }

    /**
     * Creates a new BlobFile instance by converting the given pathname string into
     * an abstract pathname.
     *
     * @param pathname
     */
    BlobFile(String pathname)
    {
        super(pathname);
    }

    /**
     * Creates a new BlobFile instance from a parent pathname string and a child
     * pathname string.
     *
     * @param parent
     * @param child
     */
    BlobFile(String parent, String child)
    {
        super(parent,child);
    }

    /**
     * Creates a new BlobFile instance by converting the given file: URI into an
     * abstract pathname.
     *
     * @param uri
     */
    BlobFile(URI uri)
    {
        super(uri);
    }
    
    /**
     * Using this function is prohibited because BlobBase uses the file name to locate the proper value
     * for a given key. 
     * 
     * This method is effectively a NO-OP.
     * 
     * <pre>
     * 
     * If you want to change the key associated with the data file, then:
     * 
     * 1) Create a new file in BlobBase using new key
     * 2) Copy contents of old file to new file
     * 3) Delete old file
     * 
     * </pre>
     * 
     * @param dest new file name (ignored)
     * @return false always
     */
    @Override
    public boolean renameTo(File dest)
    {
        return false;
    }
    
    /**
     * This operation is not supported by a file object obtained from BlobBase.
     * 
     * @return false always
     */
    @Override
    public boolean mkdir()
    {
        return false;
    }
    
    /**
     * This operation is not supported by a file object obtained from BlobBase.
     * 
     * @return false always
     */
    @Override
    public boolean mkdirs()
    {
        return false;
    }
    
}
