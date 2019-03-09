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

import org.blobbase.BlobFileIterator;
import org.blobbase.BlobBase;
import org.blobbase.exeptions.DuplicateKeyException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import org.testng.annotations.Test;

/**
 *
 */
public class BlobBaseSmallTest
{

    private static String TEST_DIR = "./dbSmallTest/files";

    private static int loopCnt = 100;

    public BlobBaseSmallTest()
    {
    }

    @org.testng.annotations.BeforeClass
    public static void setUpClass() throws Exception
    {
        deleteFiles();
    }

    @org.testng.annotations.AfterClass
    public static void tearDownClass() throws Exception
    {
    }

    @org.testng.annotations.BeforeMethod
    public void setUpMethod() throws Exception
    {
    }

    @org.testng.annotations.AfterMethod
    public void tearDownMethod() throws Exception
    {
    }

    @Test(priority = 1)
    public void BlobBaseDuplicateKeyTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase blobBase = BlobBase.getInstance(root, 2);

        File f1 = blobBase.createFile(1);

        assertNotNull(f1);

        // below should cause duplicate key exception
        try
        {
            File f2 = blobBase.createFile(1);

            throw new RuntimeException("did not catch duplicate key!");
        } catch (DuplicateKeyException e)
        {
            ;
        }

        boolean bDeleted = f1.delete();
        assertTrue(bDeleted);

        // we should now be able to create it
        File f2 = blobBase.createFile(1);
        assertNotNull(f2);

        // clean up
        bDeleted = f2.delete();
        assertTrue(bDeleted);

    }

    @Test(priority = 2)
    public void BlobBaseCreateFileTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase map = BlobBase.getInstance(root, 2);

        for (int i = 0; i < loopCnt; i++)
        {
            File f = map.createFile(i);
            assertNotNull(f);
            FileOutputStream fOut = new FileOutputStream(f);
            String data = Integer.toString(i);
            fOut.write(data.getBytes());
            fOut.close();
        }
    }
    
    @Test(priority = 3)
    public void BlobBaseReadTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase map = BlobBase.getInstance(root, 2);

        for (int i = 0; i < loopCnt; i++)
        {
            File f = map.getFile(i);
            assertNotNull(f);
            
            String data = Integer.toString(i);
                      
            FileInputStream fIn = new FileInputStream(f);
            
            byte[] buffer = new byte[(int)f.length()];
            
            fIn.read(buffer);
            
            String fileData = new String(buffer);
            fIn.close();
            assertEquals(data,fileData);
        }
    }
    
    @Test(priority = 4)
    public void BlobBaseIteratorTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase map = BlobBase.getInstance(root, 2);
        
        BlobFileIterator iterator = map.iterator();
        
        while (iterator.hasNext())
        {
            File f = iterator.next();
            assertNotNull(f);
        }

    }
    
    @Test(priority = 5)
    @SuppressWarnings("unchecked")
    public void BlobBaseSetAttributesTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase map = BlobBase.getInstance(root, 2);
        
        for (int i = 0; i < loopCnt; i++)
        {
            Map<String,Object> attrMap = map.getAttributeMap(i);
            assertNotNull(attrMap);
            
            attrMap.put("testAttr", "hello world!");
            map.setAttributeMap(i, attrMap);
        }
                
    }
    
    @Test(priority = 6)
    @SuppressWarnings("unchecked")
    public void BlobBaseGetAttributesTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase map = BlobBase.getInstance(root, 2);
        
        for (int i = 0; i < loopCnt; i++)
        {
            Map<String,Object> attrMap = map.getAttributeMap(i);
            assertNotNull(attrMap);
            
            String value = (String)attrMap.get("testAttr");
            assertNotNull(value);
            
            assertEquals(value,"hello world!");            
        }
                
    }
    
    @Test(priority = 7)
    public void BlobBaseExistsTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase map = BlobBase.getInstance(root, 2);
        
        // this should have been created earlier
        boolean bExists = map.exists(1);
        
        assertTrue(bExists);
    }
    
    @Test(priority = 8)
    public void BlobBaseDeleteTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase map = BlobBase.getInstance(root, 2);
        
        // this should have been created earlier
        boolean bdeleted = map.deleteFile(1);
        
        assertTrue(bdeleted);
    }
    
    @Test(priority = 9)
    public void BlobBasePurgeTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase map = BlobBase.getInstance(root, 2);
        
        map.purgeFiles(new Date());
        
    }
    
    @Test(priority = 10)
    public void BlobBaseEqualityTest() throws Exception
    {
        File root = new File(TEST_DIR);

        BlobBase map1 = BlobBase.getInstance(root, 2);
        BlobBase map2 = BlobBase.getInstance(root, 2);
        
        assertEquals(map1.compareTo(map2),0);               
        
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
