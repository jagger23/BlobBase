/*
 * Copyright (c) 2018 Tarokun LLC. All rights reserved.
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

import org.blobbase.BlobBase;
import org.blobbase.io.BlobBaseInputStream;
import org.blobbase.io.BlobBaseOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 */
public class BlobBaseInputOutputTest
{

    private static String TEST_DIR = "./dbIOTest/files";

    public BlobBaseInputOutputTest()
    {
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        deleteFiles();
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
    }

    @BeforeMethod
    public void setUpMethod() throws Exception
    {
    }

    @AfterMethod
    public void tearDownMethod() throws Exception
    {
    }

    @Test(priority = 1)
    public void OutputStreamTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase blobBase = BlobBase.getInstance(root);
        
        BlobBaseOutputStream blobOut = new BlobBaseOutputStream(blobBase,"my key");
        
        byte[] buffer = new byte[100];
        blobOut.write(buffer);
        blobOut.close();

    }
        
    @Test(priority = 2)
    public void OutputCompressedStreamTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase blobBase = BlobBase.getInstance(root);
        blobBase.setCompressed();
        
        BlobBaseOutputStream blobOut = new BlobBaseOutputStream(blobBase,"my compressed key");
        
        byte[] buffer = "test compression".getBytes();
        blobOut.write(buffer);
        blobOut.close();

    }
    
    @Test(priority = 3)
    public void InputCompressedStreamTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase blobBase = BlobBase.getInstance(root);
        
        BlobBaseInputStream blobIn = new BlobBaseInputStream(blobBase,"my compressed key");
        
        String testString = "test compression";
        byte[] buffer = new byte[100];
        int numBytes = blobIn.read(buffer);
        blobIn.close();
        String resultString = new String(buffer,0,numBytes);
        
        assertEquals(numBytes,testString.length());
        assertEquals(testString,resultString);

    }
    
    @Test(priority = 4)
    public void InputStreamTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase blobBase = BlobBase.getInstance(root);
        
        BlobBaseInputStream blobIn = new BlobBaseInputStream(blobBase,"my key");
        
        byte[] buffer = new byte[100];
        int numBytes = blobIn.read(buffer);
        blobIn.close();
        
        assertEquals(numBytes,buffer.length);

    }
    
    @Test(priority = 5)
    public void InputStreamDeleteTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase blobBase = BlobBase.getInstance(root);
        
        String key = "my key";
        
        BlobBaseInputStream blobIn = new BlobBaseInputStream(blobBase,key);
        
        byte[] buffer = new byte[100];
        int numBytes = blobIn.read(buffer);
        blobIn.close();
        
        assertEquals(numBytes,buffer.length);
                
        boolean bDeleted = blobBase.deleteFile(key);
        assertTrue(bDeleted);
    }
    
    @Test(priority = 6)
    public void CompressedInputStreamDeleteTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase blobBase = BlobBase.getInstance(root);
        
        String key = "my compressed key";
        
        BlobBaseInputStream blobIn = new BlobBaseInputStream(blobBase,key);
        
        byte[] buffer = new byte[100];
        int numBytes = blobIn.read(buffer);
        blobIn.close();
        
        boolean bDeleted = blobBase.deleteFile(key);
        assertTrue(bDeleted);
    }
    
    private static void deleteFiles() throws Exception
    {
        System.out.println("deleting files in database");
        File f = new File(TEST_DIR);
        if (f.exists())
        {
            Path pathToBeDeleted = Paths.get(TEST_DIR);

            Files.walk(pathToBeDeleted)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);

            assertFalse("Directory still exists", Files.exists(pathToBeDeleted));
        }
        f.mkdirs();
    }
}
