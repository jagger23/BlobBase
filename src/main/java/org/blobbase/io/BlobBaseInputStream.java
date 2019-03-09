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
package org.blobbase.io;

import org.blobbase.BlobBase;
import org.blobbase.exeptions.KeyNotFoundException;
import org.blobbase.utils.FileUtils;
import org.blobbase.utils.FileUtils.CompressionType;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.InflaterInputStream;

/**
 * A convenience class for obtaining an InputStream in BlobBase.
 * 
 * This class transparently handles compressed files in BlobBase.
 * 
 */
public class BlobBaseInputStream extends InputStream
{
    private BlobBase blobBase;
    private Object key;
    protected InputStream in;
    
    public BlobBaseInputStream(BlobBase blobBase, Object key)
    {
        super();
        this.blobBase = blobBase;
        this.key = key;
    }
    
    public int read() throws IOException
    {
        if (in == null)
        {
            File f = blobBase.getFile(key);
            if (f == null)
            {
                throw new KeyNotFoundException(key.toString());
            }
            
            in = new FileInputStream(f) ;
            in = new BufferedInputStream(in);
            
            if (blobBase.isCompressed())
            {
                // they may have turned on compression after storing files
                // into BlobBase. Look for ZLIB signature before assuming it is 
                // compressed.
                CompressionType compType = FileUtils.getCompressionType(f);
                
                if (compType == CompressionType.ZLIB)
                {
                    in = new InflaterInputStream(in);
                }
            }
        }
        return in.read();
    }
    
    @Override
    public void close() throws IOException
    {
        if (in != null)
        {
            in.close();
        }
    }
}
