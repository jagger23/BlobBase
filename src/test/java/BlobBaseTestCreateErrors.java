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
import org.blobbase.exeptions.IllegalDatabaseModificationException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import static org.testng.AssertJUnit.assertFalse;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 */
public class BlobBaseTestCreateErrors
{

    private static String TEST_DIR = "./db1/files";

    public BlobBaseTestCreateErrors()
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
    public void testDBModError() throws Exception
    {
        /*
        File root = new File(TEST_DIR);
        BlobBase map = new BlobBase(root,3);
        
        try
        {
            // this is illegal, can not change starting prime once database is created 
            map = new BlobBase(root,5);
            throw new RuntimeException("did not get expected error");
        }
        catch (IllegalDatabaseModificationException e)
        {
        }
        */

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
