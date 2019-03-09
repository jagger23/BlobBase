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
import org.blobbase.exeptions.DuplicateKeyException;
import org.blobbase.exeptions.KeyNotFoundException;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;

/**
 * A convenience class for obtaining an OutputStream to BlobBase.
 * 
 * This class transparently handles compressed files in BlobBase.
 */
public class BlobBaseOutputStream extends OutputStream
{
    private BlobBase blobBase;
    private Object key;
    protected OutputStream out;
    
    /**
     * Constructs a BlobBaseOutputStream OutputStream
     * 
     * @param blobBase BlobBase
     * @param key to write to
     */
    public BlobBaseOutputStream(BlobBase blobBase, Object key) throws IOException
    {
        super();
        this.blobBase = blobBase;
        this.key = key;
        
            File f = null;
            
            try
            {
                f = blobBase.createFile(key);
            }
            catch (DuplicateKeyException e)
            {
                f = blobBase.getFile(key);
            }
            
            if (f == null)
            {
                // did someone delete the file before we could use it????
                throw new KeyNotFoundException();
            }
            
            out = new FileOutputStream(f);
            
            out = new BufferedOutputStream(out);
            
            if (blobBase.isCompressed())
            {
                out = new DeflaterOutputStream(out);
            }        
    }

    /**
     * Writes the specified byte to this output stream.
     *
     * @param b byte to write
     */
    @Override
    public void write(int b) throws IOException
    {
        /*
        if (out == null)
        {
            File f = null;
            
            try
            {
                f = blobBase.createFile(key);
            }
            catch (DuplicateKeyException e)
            {
                f = blobBase.getFile(key);
            }
            
            if (f == null)
            {
                // did someone delete the file before we could use it????
                throw new KeyNotFoundException();
            }
            
            out = new FileOutputStream(f);
            
            out = new BufferedOutputStream(out);
            
            if (blobBase.isCompressed())
            {
                out = new DeflaterOutputStream(out);
            }
            
        }
        */
        out.write(b);
    }
    
    /**
     * Close underlying stream.
     * 
     * @throws IOException if an error occurs closing the underlying stream
     */
    @Override
    public void close() throws IOException
    {
        if (out != null)
        {
            out.close();
        }
    }
    
    /**
     * Flush underlying stream.
     * 
     * @throws IOException if an error occurs flushing the underlying stream
     */
    @Override
    public void flush() throws IOException
    {
        if (out != null)
        {
            out.flush();
        }
    }
}
