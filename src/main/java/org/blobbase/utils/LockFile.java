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
package org.blobbase.utils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Obtain a file lock. 
 * 
 * The file lock operates at the VM level and does not prevent multiple java threads from accessing file.
 * 
 */
public class LockFile
{

    private FileLock lock;  // this only blocks other vm's
    private FileChannel channel;
    private boolean autoClose;

    public LockFile(FileChannel channel)
    {
        this.channel = channel;
    }

    public LockFile(FileOutputStream out)
    {
        this(out.getChannel());
    }

    public boolean lock(boolean bWait4Lock) throws IOException
    {
        if (bWait4Lock)
        {
            while (true)
            {
                try
                {
                    lock = channel.lock();
                    return true;
                } catch (OverlappingFileLockException e)
                {
                    try
                    {
                        Thread.sleep(100);
                    } catch (InterruptedException ie)
                    {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } else
        {
            try
            {
                lock = channel.tryLock();

                if (lock != null && lock.isValid())
                {
                    return true;
                }

            } catch (OverlappingFileLockException e)
            {
            }
        }

        return false;
    }

    public void unlock() throws IOException
    {
        try
        {
            if (lock != null)
            {
                lock.release();
                if (autoClose)
                {
                    closeChannel();
                }
            }
        } catch (ClosedChannelException e)
        {
            ; // do nothing
        }
    }
    
    public void closeChannel() throws IOException
    {
        channel.close();
    }

    /**
     * Determine status of channel auto close.
     * @return the autoClose flag
     */
    public boolean isAutoClose()
    {
        return autoClose;
    }

    /**
     * If true the underlying channel will be automatically closed when lock is freed.
     * 
     * @param autoClose the autoClose to set
     */
    public void setAutoClose(boolean autoClose)
    {
        this.autoClose = autoClose;
    }
}
