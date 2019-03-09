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

import org.blobbase.BlobBase;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import org.testng.annotations.Test;

/**
 *
 */
public class BlobBaseMutiThreadTest
{

    private static String TEST_DIR = "./dbMutiThreadTest/files";

    public BlobBaseMutiThreadTest()
    {
    }

    @org.testng.annotations.BeforeClass
    public static void setUpClass() throws Exception
    {
        deleteFiles();
    }

    @org.testng.annotations.AfterClass
    public static void tearDownClass()
    {
    }


    @Test(priority = 1)
    public void threadTest() throws Exception
    {
        int x = 0;
        Worker[] workers = new Worker[100];

        for (int i = 0; i < workers.length; i++)
        {
            workers[i] = new Worker(x++);
            workers[i].start();
        }

        Thread.sleep(60000);

        for (int i = 0; i < workers.length; i++)
        {
            Worker worker = workers[i];
            worker.setStop(true);
        }

        Thread.sleep(1000);

        for (int i = 0; i < workers.length; i++)
        {
            Worker worker = workers[i];
            worker.join();
        }

        System.out.println("Done...");

    }

    private class Worker extends Thread
    {

        private boolean stop = false;
        private int x = 0;
        private int loopCnt = 100;

        public Worker(int x)
        {
            this.x = x * loopCnt*10;
        }

        @SuppressWarnings("unchecked")
        public void run()
        {
            try
            {

                File root = new File(TEST_DIR);
                BlobBase map = BlobBase.getInstance(root);

                String data = "Hello World ";

                while (!stop)
                {
                    Random random = new Random();

                    int delay = random.nextInt(1000);
                    Thread.sleep(delay);

                    for (int i = 0; i < loopCnt; i++)
                    {
                        String key = "key" + x + i;
                        
                            
                        File f1 = map.createFile(key);
                        
                        assertNotNull(f1);
                        
                        FileOutputStream fileOut = new FileOutputStream(f1);
                        String d = data + x + i;
                        fileOut.write(d.getBytes());
                        fileOut.close();
                        
                        
                        Map<String,Object> attrMap = new HashMap();
                        attrMap.put("testAttr", "123");
                                                
                        map.setAttributeMap(key, attrMap);
                        
                        delay = random.nextInt(100);
                        Thread.sleep(delay);
                    }

                    delay = random.nextInt(1000);
                    Thread.sleep(delay);

                    for (int i = 0; i < loopCnt; i++)
                    {
                        String key = "key" + x + i;
                        File f1 = map.getFile(key);
                        assertNotNull(f1);
                        
                        FileInputStream fileIn = new FileInputStream(f1);
                        
                        byte[] buffer = new byte[(int)f1.length()];
                        
                        fileIn.read(buffer);
                        
                        String value = new String(buffer);
                        fileIn.close();

                        assertEquals(data + x + i, value);
                        
                        Map<String,Object> attrMap = map.getAttributeMap(key);
                        String attrValue = (String)attrMap.get("testAttr");
                        
                        assertEquals("123",attrValue);
                                
                        delay = random.nextInt(100);
                        Thread.sleep(delay);
                    }

                    delay = random.nextInt(1000);
                    Thread.sleep(delay);

                    for (int i = 0; i < loopCnt; i++)
                    {
                        File f1 = map.getFile("key" + x + i);
                        assertNotNull(f1);
                        if (!f1.delete())
                        {
                            System.out.println("delete failed: key" + x + i);
                        }
                        delay = random.nextInt(100);
                        Thread.sleep(delay);
                    }

                    delay = random.nextInt(1000);
                    Thread.sleep(delay);

                    for (int i = 0; i < loopCnt; i++)
                    {
                        File f1 = map.getFile("key" + x + i);
                        if (f1 != null)
                        {
                            System.out.println("found key: key" + x + i);
                            System.out.println("file:"+f1.toString());
                        }
                        assertNull(f1);
                    }

                }
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        /**
         * @return the stop
         */
        public boolean isStop()
        {
            return stop;
        }

        /**
         * @param stop the stop to set
         */
        public void setStop(boolean stop)
        {
            this.stop = stop;
        }

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
