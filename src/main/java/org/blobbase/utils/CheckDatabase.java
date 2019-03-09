/*
 * Copyright (c) 2019 Tarokun LLC. All rights reserved.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * A utility class for checking database consistency.
 * 
 */
public class CheckDatabase
{

    final private static String MAP_FILE = ".key.map";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception
    {
        String dbPath = null;

        for (String s : args)
        {
            if (s.startsWith("-database="))
            {
                dbPath = s.substring("-database=".length());
            }
        }

        if (dbPath == null)
        {
            usage();
            System.exit(-1);
        }

        File dbDir = new File(dbPath);
        if (!dbDir.exists() || !dbDir.isDirectory())
        {
            throw new IllegalArgumentException(dbPath);
        }

        CheckDatabase checkDb = new CheckDatabase(dbDir);

        checkDb.check();
        
        if (checkDb.getProblemCnt()>0)
        {
            System.out.println("problems found ("+checkDb.getProblemCnt()+")");
        }
        else
        {
            System.out.println("nNo problems found");
        }
        
        System.out.println("database check finished...");

    }

    private static void usage()
    {
        System.out.println("Missing a required Parameter:");
        System.out.println("Usage: \n\t-database=<database path>");
    }

    private File dbDir;
    private boolean compressed = false;
    private int problemCnt=0;

    public CheckDatabase(File dbDir)
    {
        this.dbDir = dbDir;
    }

    public void check() throws FileNotFoundException, IOException, ClassNotFoundException
    {
        checkDbInfo();
        checkMaps(dbDir);
    }

    public void checkDbInfo() throws FileNotFoundException, IOException
    {
        File dbInfo = new File(dbDir, ".dbInfo");
        
        System.out.println("checking:"+dbDir.getPath());

        FileInputStream dbInfoIS = new FileInputStream(dbInfo);
        Properties properties = new Properties();
        properties.load(dbInfoIS);
        dbInfoIS.close();

        int startingPrime = Integer.parseInt(properties.getProperty("startingPrime", "0"));

        if (!PrimeNumber.isPrime(startingPrime))
        {
            System.err.println("startingPrime in " + dbInfo.getName() + " is not prime");
            problemCnt++;
        }

        compressed = Boolean.parseBoolean(properties.getProperty("compressed", "false"));
    }

    public void checkMaps(File currentDir) throws FileNotFoundException, IOException, ClassNotFoundException
    {
        // get keymap
        File mapFile = new File(currentDir, MAP_FILE);

        if (!mapFile.exists())
        {
            throw new FileNotFoundException(mapFile.getAbsolutePath());
        }

        Map<Object, File> map = (Map<Object, File>) FileUtils.readObject(mapFile, 0, true);

        Set<Object> keySet = map.keySet();
        for (Object o : keySet)
        {
            File keyFile = map.get(o);
            File f = new File(mapFile.getParentFile(), keyFile.getName());
            if (!f.exists())
            {
                System.err.println("key:" + o.toString() + " with no file found: " + f.getAbsolutePath() + " found");
                problemCnt++;
            }
        }

        File[] files = currentDir.listFiles();

        for (File f : files)
        {
            if (f.getName().startsWith("."))
            {
                continue;
            }

            if (f.isFile())
            {

                boolean bFound = false;
                // see if file is in map
                Set<Entry<Object, File>> entrySet = map.entrySet();
                for (Entry<Object, File> e : entrySet)
                {
                    File kf = e.getValue();
                    if (f.getName().equals(kf.getName()))
                    {
                        bFound = true;
                        break;
                    }
                }

                if (!bFound)
                {
                    System.err.println("orphaned file: " + f.getAbsolutePath() + " found");
                    problemCnt++;
                }
            } else if (f.isDirectory())
            {
                checkMaps(f);
            }

        }
    }

    /**
     * @return the problemCnt
     */
    public int getProblemCnt()
    {
        return problemCnt;
    }

}
