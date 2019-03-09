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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.zip.DeflaterOutputStream;

/**
 * File utilities for performing various tasks.
 *
 */
public class FileUtils
{

    public enum CompressionType
    {
        ZLIB, Z, GZIP, LZH, UNKNOWN
    }

    /**
     * Attempt to determine compression of file. This is a best attempt effort
     * that examines the first few bytes of the file for the presence of a
     * 'magic number' that indicated the compression type.
     *
     * The caller should be aware that false 'positives' can occur since the the
     * 'magic number' might appear naturally at the beginning of binary file
     * that is if fact bot compressed at all.
     *
     * @param file
     * @return CompressionType which maybe unknown.
     * @throws FileNotFoundException if file does not exist.
     * @throws IOException if an error occurs reading file.
     */
    public static CompressionType getCompressionType(File file) throws FileNotFoundException, IOException
    {
        if (file.length() < 2)
        {
            return CompressionType.UNKNOWN;
        }

        FileInputStream fileIn = new FileInputStream(file);
        DataInputStream dataIn = new DataInputStream(fileIn);
        try {
            int b1 = dataIn.readUnsignedByte();
            int b2 = dataIn.readUnsignedByte();

            if (b1 == 0x1f) {
                // 1F 8B GZIP
                // 1F 9D Lempel-Ziv-Welch algorithm  (.Z)
                // 1F A0 LZH
                if (b2 == 0x8b) {
                    return CompressionType.GZIP;
                }

                if (b2 == 0x9d) {
                    return CompressionType.Z;
                }

                if (b2 == 0xa0) {
                    return CompressionType.LZH;
                }
            } else if (b1 == 0x78) {
                // 78 01 ZLIB no compression
                // 78 9C ZLIB default compression
                // 78 DA ZLIB best compression

                if (b2 == 0x01 || b2 == 0x9c || b2 == 0xDA) {
                    return CompressionType.ZLIB;
                }
            }
        }
        finally
        {
            dataIn.close();
            fileIn.close();
        }

        


        // probably not compressed at all
        return CompressionType.UNKNOWN;

    }

    /**
     * Read java object starting from offset.
     *
     * @param data file containing object
     * @param offset in bytes
     * @param bLock if true lock file before reading (VM level - does not protect against other threads)
     * @return Object read
     * @throws IOException if error occurs reading file
     * @throws ClassNotFoundException if class of object not in class path
     */
    public static Object readObject(File data, long offset, boolean bLock) throws IOException, ClassNotFoundException
    {
        Object object = null;
        InputStream fis = null;

        LockFile lock = null;

        try
        {
            if (bLock)
            {
                RandomAccessFile raf = new RandomAccessFile(data, "rw");

                FileOutputStream fos = new FileOutputStream(raf.getFD());
                // we don;t want anyone writting to file while we read it
                lock = new LockFile(fos);
                lock.setAutoClose(true);
                lock.lock(true);

                fis = Channels.newInputStream(raf.getChannel());
            } else
            {
                fis = new FileInputStream(data);
            }
            //FileInputStream fis = new FileInputStream(data);
            fis.skip(offset);

            BufferedInputStream bufIn = new BufferedInputStream(fis);
            ObjectInputStream ois = new ObjectInputStream(bufIn);
            object = ois.readObject();

            ois.close();
            bufIn.close();
            fis.close();
        } finally
        {
            if (lock != null)
            {
                lock.unlock();
            }
        }

        return object;
    }

    /**
     * Append an object to data file
     *
     * @param data file
     * @param object to append
     * @return offset of object into file
     * @throws IOException if error occurs appending object
     */
    public static long appendObject(File data, Object object) throws IOException
    {
        long offset = data.length();
        writeObject(data, object, offset);
        return offset;
    }

    /**
     * Write object at offset
     *
     * @param data file
     * @param object to write
     * @param offset offset into datafile
     * @throws IOException if an error occurs
     */
    public static void writeObject(File data, Object object, long offset) throws IOException
    {

        LockFile lock = null;

        try
        {
            RandomAccessFile raf = new RandomAccessFile(data, "rw");
            raf.seek(offset);

            FileOutputStream fos = new FileOutputStream(raf.getFD());
            lock = new LockFile(fos);
            lock.setAutoClose(true);
            lock.lock(true);
            BufferedOutputStream bufOut = new BufferedOutputStream(fos);
            ObjectOutputStream oos = new ObjectOutputStream(bufOut);
            oos.writeObject(object);
            oos.close();
            bufOut.close();
            fos.close();
            raf.close();
        } finally
        {
            if (lock != null)
            {
                lock.unlock();
            }
        }

    }
    
    /**
     * Copies and compresses file.
     * 
     * @param f file to compress
     * @return Compressed copy of file
     * @throws FileNotFoundException if file does not actually exist
     * @throws IOException if an error occurs during compression operation
     */
    public static File compressFile(File f) throws FileNotFoundException, IOException
    {
        FileInputStream fileIn = new FileInputStream(f);
        BufferedInputStream buffIn = new BufferedInputStream(fileIn);

        File compFile = File.createTempFile("blobBase.", ".cmp", f.getParentFile());
        FileOutputStream fileOut = new FileOutputStream(compFile);
        BufferedOutputStream buffOut = new BufferedOutputStream(fileOut);

        DeflaterOutputStream deflatorOut = new DeflaterOutputStream(buffOut);
        while (true)
        {
            int b = buffIn.read();
            if (b == -1)
            {
                break;
            }

            deflatorOut.write(b);
        }

        deflatorOut.finish();

        buffOut.close();
        fileOut.close();

        buffIn.close();
        fileIn.close();

        return compFile;
    }

}
