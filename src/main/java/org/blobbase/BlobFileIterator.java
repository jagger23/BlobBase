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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A lazily populated iterator for listing files in database.
 * 
 */
public class BlobFileIterator implements Iterator
{
    private File root;
    private List<File> fileList = new ArrayList<File>();
    private File nextFile;
    
    public BlobFileIterator(File root) 
    {
        this.root = root;
        fileList.addAll(getFiles(root));
                
    }
    
    public boolean hasNext()
    {
        nextFile = null;
        
        // locate next file
        while (true)
        {
            if (fileList.size() == 0)
            {
                break;
            }
            
            File f = fileList.remove(0);
            
            if (f.isHidden())
            {
                // we use hidden files to store db information
                continue;
            }
            
            if (f.isFile())
            {
                nextFile = f;
                break;
            }
            else if (f.isDirectory())
            {
                fileList.addAll(getFiles(f));
            }
        }
        
        return (nextFile != null);
    }
    
    public File next()
    {
        if (nextFile == null)
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
        }
        return nextFile;
    }
    
    private static List<File> getFiles(File dir)
    {
        List<File> list = new ArrayList<File>();
        
        File[] files = dir.listFiles();
        
        for (File f: files)
        {
            list.add(f);
        }
        
        return list;
    }
}
