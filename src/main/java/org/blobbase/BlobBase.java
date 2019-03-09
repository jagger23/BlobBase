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

import org.blobbase.exeptions.CorruptDatabaseException;
import org.blobbase.exeptions.DeleteFileException;
import org.blobbase.exeptions.DuplicateKeyException;
import org.blobbase.exeptions.FileInUseException;
import org.blobbase.exeptions.IllegalDatabaseModificationException;
import org.blobbase.exeptions.MakeDirectoryException;
import org.blobbase.exeptions.OrphanedFileException;
import org.blobbase.exeptions.RelocateFileException;
import org.blobbase.exeptions.CreateFileException;
import org.blobbase.exeptions.KeyNotFoundException;
import org.blobbase.exeptions.RenameFileException;
import org.blobbase.utils.FileUtils;
import org.blobbase.utils.LockFile;
import org.blobbase.utils.PrimeNumber;
import org.blobbase.utils.Sha1Util;
import org.blobbase.utils.ThreadUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Create an abstract storage mechanism used for mapping keys to binary large
 * objects (BLOB) stored on the file system.
 *
 * The key mapping is accomplished algorithmically by creating a hash out of the
 * key and mapping it to a slot (file) in a directory. Key collisions are
 * handled by the creation of subdirectories to handle the collided keys and
 * files.
 *
 * <pre>
 *
 * Example 1: Create file
 *
 *  // location  of database
 *  File root = new File("./myDb");
 *
 *  // instantiate BlobBase
 *  BlobBase blobBase = BlobBase.getInstance(root);
 *
 *  // create a file in database
 *  File file = blobBase.createFile("my key");
 *
 *  // I can now use 'file' normally to write data.
 *  FileOutputStream fOut = new FileOutputStream(file);
 *
 *  Example 2: Read file
 *
 *  File root = new File("./myDb");
 *
 *  // setup location
 *  BlobBase blobBase = BlobBase.getInstance(root);
 *
 *  // get file from database
 *  File file = blobBase.getFile("my key");
 *  if (file == null)
 *  {
 *    System.out.println("file with key was not found");
 *  }
 *  else
 *  {
 *    // I can now use file object returned to read data
 *    FileInputStream in = new FileInputStream(file);
 *    ... do something with data ....
 *  }
 *
 *  Example 2: Delete file
 *
 *  File root = new File("./myDb");
 *
 *  // setup location
 *  BlobBase blobBase = new BlobBase(root);
 *
 *  // get file from database
 *  File file = blobBase.getFile("my key");
 *  if (file != null)
 *  {
 *    // delete just like any other file
 *    file.delete();
 *  }
 *
 *
 * </pre>
 *
 * Using Compression
 *
 * BlobBase can optionally compress files stored in it. Do do so create a
 * BlobBase and then set the compression flag.
 * <br></br>
 * You can turn compression on later after creating a database but beware -
 * Compression once enabled can not be disabled.
 *
 * <pre>
 *
 *  // set location  of database
 *  File root = new File("./myDb");
 *
 *  // instantiate BlobBase
 *  BlobBase blobBase = BlobBase.getInstance(root);
 *  // turn compression on
 *  blobBase.setCompressed();
 *
 *  OutputStream blobOut = BlobBaseOutputStream(blobBase, "myId");
 *  blobOut.write("mydata".getBytes()); // this will be automatically compressed when written
 *
 *  // To read compressed data transparently
 *  InputStream blobIn = new BlobBaseInputStream(blobBase,"myId");
 *  int b = blobIn.read();   // this byte will be uncompressed automatically wwhen read.
 *
 * </pre>
 */
public class BlobBase implements Serializable, Comparable<BlobBase>
{

    final private static long serialVersionUID = 1L;
    final private static String MAP_FILE = ".key.map";

    private File rootDir;

    private int startingPrime = 0;
    private int startingOffset = 1;

    // todo: parameterise these two options.
    private boolean bMoveFiles = true;
    // todo: implement this
    private int maxFiles;   // should be specified when bMoveFiles is false. We will use this value to compute required directory structure.

    private boolean compressed; // if true we are compressing files

    private int lockWaitSleep = 500;
    private int maxCreateFileAttempts = 10;

    private transient ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    private static HashMap<File, BlobBase> instanceMap = new HashMap<File, BlobBase>();

    public static synchronized BlobBase getInstance(File rootDir) throws IOException
    {
        BlobBase bBase = instanceMap.get(rootDir);
        if (bBase == null)
        {
            bBase = new BlobBase(rootDir);
            instanceMap.put(rootDir, bBase);
        }

        return bBase;
    }

    public static synchronized BlobBase getInstance(File rootDir, int startingPrime) throws IOException
    {
        BlobBase bBase = instanceMap.get(rootDir);
        if (bBase == null)
        {
            bBase = new BlobBase(rootDir, startingPrime);
            instanceMap.put(rootDir, bBase);
        }

        return bBase;
    }

    protected BlobBase(File rootDir) throws IllegalArgumentException, IOException
    {
        this(rootDir, PrimeNumber.primeNumbers[PrimeNumber.findIndex(8009)]);
    }

    /**
     * Create a FileMap database at location with initial directory capacity.
     *
     * @param rootDir location of files and directories
     * @param startingPrime a prime number that indicates the maximum number of
     * files/directories in root directory.
     *
     * @throws IllegalArgumentException if startingPrime is not a prime number
     * (p) in the range of 2 &gt;= p &lt;= 8011
     * @throws IOException if an error occurs in the course of setting up
     * database
     * @throws CorruptDatabaseException if unable to read/parse our '.dbInfo'
     * file
     * @throws IllegalDatabaseModificationException if you specified a different
     * startingPrime for a previously created database
     */
    protected BlobBase(File rootDir, int startingPrime) throws IllegalArgumentException, IOException, CorruptDatabaseException, IllegalDatabaseModificationException
    {
        if (rootDir == null || rootDir.isFile())
        {
            throw new IllegalArgumentException();
        }

        this.rootDir = rootDir;

        if (!PrimeNumber.isPrime(startingPrime) || startingPrime < 2 || startingPrime > PrimeNumber.primeNumbers[PrimeNumber.primeNumbers.length - 1])
        {
            throw new IllegalArgumentException(Integer.toString(startingPrime));
        }

        this.startingPrime = startingPrime;

        this.startingOffset = PrimeNumber.findIndex(startingPrime);

        synchronized (this.getClass())
        {
            // initialize root
            File dbInfo = getDbInfo();
            if (!dbInfo.exists())
            {
                Properties properties = new Properties();
                properties.setProperty("startingPrime", Integer.toString(startingPrime));
                FileOutputStream fOut = new FileOutputStream(dbInfo);
                properties.store(fOut, "DB Settings - DO NOT MODIFY UNLESS YOU KNOW WHAT YOU ARE DOING!!!");
                fOut.close();
            } else
            {
                Properties properties = getProperties(dbInfo);
                try
                {
                    this.startingPrime = Integer.parseInt(properties.getProperty("startingPrime"));
                    this.startingOffset = PrimeNumber.findIndex(this.startingPrime);
                    compressed = Boolean.parseBoolean(properties.getProperty("compressed", "false"));

                } catch (NumberFormatException ne)
                {
                    ne.printStackTrace();
                    throw new CorruptDatabaseException();
                }
            }
        }
    }

    /**
     * Returns a empty atomically created file.
     *
     * @param key the key to use for obtaining the file
     * @return file created in databaseS
     * @throws IOException if an error occurs
     * @throws DuplicateKeyException if a file exists with the specified key
     * already
     * @throws CreateFileException if unable to create file
     * @throws FileInUseException if unable to get a file lock
     * @throws CorruptDatabaseException if missing .key.map file
     */
    public File createFile(Object key) throws IOException, DuplicateKeyException, CreateFileException, FileInUseException, CorruptDatabaseException
    {
        return createFile(key, 1);
    }

    @SuppressWarnings("unchecked")
    protected File createFile(Object key, int depth) throws IOException, DuplicateKeyException, CreateFileException, FileInUseException, CorruptDatabaseException
    {
        if (depth > maxCreateFileAttempts)
        {
            throw new FileInUseException();
        }

        byte[] hash = Sha1Util.hashBytes(key.toString().getBytes());

        File file = null;

        try
        {
            rwLock.writeLock().lock();

            boolean isNew = false;
            while (!isNew)
            {
                file = locateFile(key, startingOffset);
                if (file == null)
                {
                    // Houston: we have a problem
                    throw new IOException("could not acquire file");
                }

                Map<Object, File> map = null;

                File mapFile = new File(file.getParentFile(), MAP_FILE);
                if (mapFile.exists())
                {
                    try
                    {
                        map = (Map<Object, File>) FileUtils.readObject(mapFile, 0, true);
                    } catch (ClassNotFoundException e)
                    {
                        throw new IOException(e);
                    }
                } else
                {
                    map = new HashMap<Object, File>();
                }

                if (file.exists())
                {
                    // two reasons this can happen:
                    // a) they user has asked to create a file with the same key, in which case we return an error
                    // b) there is a key collision with a file already created, in which case we must add a new level
                    //    to the dirctory hierarchy.
                    //
                    // To determine which (A or B) we must consult our key map located in same directory as the file
                    // in question.

                    if (!mapFile.exists())
                    {
                        // this should never happen, if the file exists so should map file
                        throw new CorruptDatabaseException();
                    }

                    Object prevValue = map.get(key);
                    if (prevValue != null)
                    {
                        throw new DuplicateKeyException();
                    }

                    // seems we need to create a subdirectory so that we can subdivide they key space.
                    if (!bMoveFiles)
                    {
                        // seems we dont want this behavior
                        throw new RelocateFileException(file);
                    }

                    // Get a lock to block other vm's while i'm doing the split
                    File lockFile = getLockFile(file);

                    LockFile lock = getLock(lockFile);
                    try
                    {
                        if (!lock.lock(false))
                        {
                            // someone else is messing with the file
                            ThreadUtils.safeSleep(lockWaitSleep * depth);
                            return createFile(key, depth++);
                        }

                        // create temp directory
                        File temp = File.createTempFile("split.", ".tmp", file.getParentFile());

                        File tempDir = new File(file.getParentFile(), temp.getName());
                        temp.delete();

                        if (!tempDir.mkdir())
                        {
                            throw new MakeDirectoryException(tempDir);
                        }

                        File destFile = new File(tempDir, file.getName() + ".tmp");

                        // move file 
                        Files.move(file.toPath(), destFile.toPath(), StandardCopyOption.ATOMIC_MOVE);

                        // rename dirctory
                        if (!temp.renameTo(file))
                        {
                            throw new RenameFileException(temp);
                        }

                        Object prevKey = null;
                        File prevFile = null;
                        // find the key used to save the original file
                        Set<Map.Entry<Object, File>> entries = map.entrySet();
                        Iterator<Map.Entry<Object, File>> iterator = entries.iterator();
                        while (iterator.hasNext())
                        {
                            Map.Entry<Object, File> entry = iterator.next();
                            if (entry.getValue().getName().equals(file.getName()))
                            {
                                prevKey = entry.getKey();
                                prevFile = entry.getValue();
                                break;
                            }
                        }

                        if (prevKey == null)
                        {
                            throw new OrphanedFileException(file);
                        }

                        // need to get a new file name
                        File movedFileName = createFile(prevKey);

                        // extract parts
                        File parentDir = movedFileName.getParentFile();
                        File newName = new File(parentDir, movedFileName.getName());

                        // but we dont want to keep this file
                        if (!movedFileName.delete())
                        {
                            throw new DeleteFileException(movedFileName);
                        }

                        // because we are gonna rename the previous
                        File tmpFile = new File(parentDir, destFile.getName());
                        if (!tmpFile.renameTo(newName))
                        {
                            throw new RenameFileException(destFile);
                        }

                        map.remove(prevKey);

                        if (prevFile instanceof FileWithMetadata)
                        {
                            // copy over any attributes 
                            FileWithMetadata metaDataFile = (FileWithMetadata) prevFile;
                            setAttributeMap(prevKey, metaDataFile.getMap());
                        }

                    } finally
                    {
                        lock.unlock();
                        lockFile.delete();
                    }
                }

                isNew = file.createNewFile();

                map.put(key, new FileWithMetadata(file));
                FileUtils.writeObject(mapFile, map, 0);
            }
        } finally
        {
            rwLock.writeLock().unlock();
        }

        return new BlobFile(file.toURI());
    }

    /**
     * Returns file with this key or null if not present.
     *
     * @param key to use to get file with
     * @return file from database or null if no key mapping found
     * @throws IOException if an error occurs
     */
    @SuppressWarnings("unchecked")
    public File getFile(Object key) throws IOException
    {
        BlobFile blobFile = null;

        try
        {
            rwLock.readLock().lock();

            File file = locateFile(key, startingOffset);

            if (file != null)
            {
                if (!file.exists() || file.isDirectory())
                {
                    file = null;    // not really there
                }
            }

            if (file != null)
            {
                File mapFile = new File(file.getParentFile(), MAP_FILE);
                if (mapFile.exists())
                {
                    try
                    {
                        Map<Object, File> map = (Map<Object, File>) FileUtils.readObject(mapFile, 0, true);
                        // verify that this file is for this key
                        File keyFile = map.get(key);
                        
                        if (keyFile == null)
                        {
                            return null;
                        }

                        // because database may have been moved we strip path info and just use name
                        File tFile = new File(file.getParentFile(), keyFile.getName());
                        if (!tFile.equals(file))
                        {
                            // file maps to different key
                            return null;
                        }

                    } catch (ClassNotFoundException e)
                    {
                        throw new IOException(e);
                    }
                } else
                {
                    // this should never happen, if the file exists so should map file
                    throw new CorruptDatabaseException();
                }

                blobFile = new BlobFile(file.toURI());
            }
        } finally
        {
            rwLock.readLock().unlock();
        }

        return blobFile;
    }

    /**
     * A convenience method for deleting file associated with key.
     *
     * @param key of file to delete
     * @return true if file was successfully deleted or if it never actual
     * existed.
     * @throws IOException if an error occurs
     */
    @SuppressWarnings("unchecked")
    public boolean deleteFile(Object key) throws IOException
    {
        try
        {
            rwLock.writeLock().lock();
            File file = getFile(key);
            if (file != null)
            {
                File lockFile = getLockFile(file);
                LockFile lock = getLock(lockFile);

                try
                {
                    lock.lock(true);

                    File mapFile = new File(file.getParentFile(), MAP_FILE);
                    if (mapFile.exists())
                    {
                        try
                        {
                            Map<Object, File> map = (Map<Object, File>) FileUtils.readObject(mapFile, 0, true);
                            map.remove(key);
                            FileUtils.writeObject(mapFile, map, 0);
                        } catch (ClassNotFoundException e)
                        {
                            throw new IOException(e);
                        }
                    } else
                    {
                        // this should never happen, if the file exists so should map file
                        throw new CorruptDatabaseException();
                    }

                    return file.delete();

                } finally
                {
                    if (lock != null)
                    {
                        lock.unlock();
                    }

                    if (lockFile != null)
                    {
                        lockFile.delete();
                    }
                }

            }
        } finally
        {
            rwLock.writeLock().unlock();
        }

        return true;
    }

    /**
     * A convenience method for checking for the presence of a file in Blobase.
     *
     * @param key of the file
     * @return true if a file that maps to that key is present, false otherwise.
     * @throws IOException if an error occurs
     */
    public boolean exists(Object key) throws IOException
    {
        File f = getFile(key);
        if (f != null)
        {
            return true;
        }

        return false;
    }

    /**
     * A convenience method for obtaining the size of a file.
     *
     * @param key of file to get size of
     * @return size of file or -1 if a file for the key is not present
     * @throws IOException if an error occurs
     */
    public long getlength(Object key) throws IOException
    {
        File f = getFile(key);
        if (f != null)
        {
            return f.length();
        }

        return -1;
    }

    /**
     * Returns a lazily populated Iterator for files stored in database.
     *
     * The order of files returned is not guaranteed.
     *
     * While the iterator is thread safe the caller should be aware that in an
     * active system files maybe returned that could possibly have been moved or
     * deleted by other threads in the course of using this iterator.
     *
     * @return FileIterator a File iterator
     */
    public BlobFileIterator iterator()
    {
        return new BlobFileIterator(getRootDir());
    }

    /**
     * This is a convenience method to delete files with timestamps before the
     * specified date.
     *
     * This method examines the modification date of the files in the database
     * and deletes those that are before the specified date.
     *
     * This method does not throw an error if a file can not be deleted for some
     * reason but instead marks if for deletion when the VM terminates.
     *
     * @param date delete files
     */
    public void purgeFiles(Date date)
    {

        BlobFileIterator iterator = iterator();
        while (iterator.hasNext())
        {
            File f = iterator.next();

            long timestamp = f.lastModified();

            Date modDate = new Date(timestamp);

            if (f.exists() && modDate.before(date))
            {
                f.delete();
                f.deleteOnExit();
            }
        }
    }

    /**
     * Set file attributes. All attributes must implement Serializable
     * interface.
     *
     * @param key of stored object
     * @return Map of attributes
     * @throws KeyNotFoundException if object not found in database.
     * @throws IOException if an error occurs
     */
    @SuppressWarnings("unchecked")
    public Map getAttributeMap(Object key) throws KeyNotFoundException, IOException
    {
        if (key == null)
        {
            throw new IllegalArgumentException();
        }

        Map<String, Object> attributeMap = null;

        try
        {
            rwLock.readLock().lock();

            File file = locateFile(key, startingOffset);

            if (file != null)
            {
                if (!file.exists() || file.isDirectory())
                {
                    throw new KeyNotFoundException(key.toString());
                }
            }

            if (file != null)
            {
                File mapFile = new File(file.getParentFile(), MAP_FILE);
                if (mapFile.exists())
                {
                    try
                    {
                        Map<Object, File> map = (Map<Object, File>) FileUtils.readObject(mapFile, 0, true);
                        // verify that this file is for this key
                        File keyFile = map.get(key);
                        // because database may have been moved we strip path info and just use name
                        File tFile = new File(file.getParentFile(), keyFile.getName());
                        if (!tFile.equals(file))
                        {
                            // file maps to different key
                            throw new KeyNotFoundException(key.toString());
                        }

                        if (keyFile instanceof FileWithMetadata)
                        {
                            FileWithMetadata fileWithMetadata = (FileWithMetadata) ((FileWithMetadata) keyFile);
                            attributeMap = fileWithMetadata.getMap();
                        } else
                        {
                            // file was created with earlier version of software
                            attributeMap = new HashMap();   // create an empty one
                        }

                    } catch (ClassNotFoundException e)
                    {
                        throw new IOException(e);
                    }
                } else
                {
                    // this should never happen, if the file exists so should map file
                    throw new CorruptDatabaseException();
                }

            }
        } finally
        {
            rwLock.readLock().unlock();
        }

        return attributeMap;
    }

    /**
     * Set object attributes.
     *
     * @param key of object
     * @param attributeMap map of attributes to set. All attributes mut
     * implement Serializable interface.
     * @throws KeyNotFoundException if object not found.
     * @throws IOException if error occurs
     */
    @SuppressWarnings("unchecked")
    public void setAttributeMap(Object key, Map<String, Object> attributeMap) throws KeyNotFoundException, IOException
    {
        if (key == null || attributeMap == null)
        {
            throw new IllegalArgumentException();
        }

        try
        {
            rwLock.writeLock().lock();

            File file = locateFile(key, startingOffset);

            if (file != null)
            {
                if (!file.exists() || file.isDirectory())
                {
                    throw new KeyNotFoundException(key.toString());
                }
            }

            if (file != null)
            {
                File mapFile = new File(file.getParentFile(), MAP_FILE);
                if (mapFile.exists())
                {
                    try
                    {
                        Map<Object, File> map = (Map<Object, File>) FileUtils.readObject(mapFile, 0, true);
                        // verify that this file is for this key
                        File keyFile = map.get(key);
                        // because database may have been moved we strip path info and just use name
                        File tFile = new File(file.getParentFile(), keyFile.getName());
                        if (!tFile.equals(file))
                        {
                            // file maps to different key
                            throw new KeyNotFoundException(key.toString());
                        }

                        FileWithMetadata fileWithMetadata = new FileWithMetadata(file);
                        fileWithMetadata.setMap(attributeMap);

                        map.put(key, fileWithMetadata);
                        FileUtils.writeObject(mapFile, map, 0);
                    } catch (ClassNotFoundException e)
                    {
                        throw new IOException(e);
                    }
                } else
                {
                    // this should never happen, if the file exists so should map file
                    throw new CorruptDatabaseException();
                }

            }
        } finally
        {
            rwLock.writeLock().unlock();
        }

    }

    /**
     *
     * Searches for file with key starting at specified depth.
     *
     *
     * @param key the key of the file we want to locate
     * @param depth starting directory depth
     * @return existing File or new File
     */
    protected File locateFile(Object key, int depth)
    {
        int prime = PrimeNumber.primeNumbers[depth++];

        byte[] keyBytes = Sha1Util.hashBytes(key.toString().getBytes());

        int index = getFileNumber(keyBytes, prime);

        String nodeName = getFileName(index);

        File root = getRootDir();
        File node = new File(root, nodeName);

        while (node.exists() && node.isDirectory())
        {
            prime = PrimeNumber.primeNumbers[depth++];
            index = getFileNumber(keyBytes, prime);
            nodeName = getFileName(index);
            root = node;
            node = new File(root, nodeName);
        }

        return node;
    }

    /**
     * Get the hash bucket that the specified key maps to.
     *
     * @param key the key to use to compute bucket
     * @param prime prime number to use to compute bucket
     * @return number indicating the hash bucket.
     */
    protected int getFileNumber(byte[] key, int prime)
    {
        BigInteger bigInt = new BigInteger(key);
        BigInteger nodeIndex = bigInt.mod(new BigInteger(Integer.toString(prime)));
        int index = nodeIndex.intValue();
        return index;
    }

    /**
     * Format a file name for the given id.
     *
     * @param id numeric id of file (bucket)
     * @return String the formated file name
     */
    protected static String getFileName(int id)
    {
        String strName = "" + id;

        strName = String.format("%08d", id);

        return strName;
    }

    /**
     * @return the compressed
     */
    public boolean isCompressed()
    {
        return compressed;
    }

    /**
     * Enable database compression
     *
     * @throws IOException if error occurs updating setting.
     */
    public void setCompressed() throws IOException
    {
        this.compressed = true;

        File dbInfo = getDbInfo();
        Properties properties = getProperties(dbInfo);
        properties.setProperty("compressed", "true");

        FileOutputStream fOut = new FileOutputStream(dbInfo);
        properties.store(fOut, "DB Initializations");
        fOut.close();
    }

    /**
     * Compare two BlobBases for equality.
     *
     * @param blobBase BlobBase to compare with.
     * @return a negative integer, zero, or a positive integer as this object is
     * less than, equal to, or greater than the specified object.
     */
    @Override
    public int compareTo(BlobBase blobBase)
    {
        return this.rootDir.compareTo(blobBase.rootDir);
    }

    /**
     * Get the database information file.
     *
     * @return file
     */
    private File getDbInfo()
    {
        File dbInfo = new File(this.getRootDir(), ".dbInfo");
        return dbInfo;
    }

    /**
     * Load properties file
     *
     * @param dbInfo
     * @return Properties
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static Properties getProperties(File dbInfo) throws FileNotFoundException, IOException
    {
        FileInputStream fIn = new FileInputStream(dbInfo);
        Properties properties = new Properties();
        properties.load(fIn);

        fIn.close();

        return properties;
    }

    /**
     * @return the rootDir
     */
    public File getRootDir()
    {
        return rootDir;
    }

    private File getLockFile(File file) throws FileNotFoundException
    {
        File lockFile = new File(file.getParentFile(), file.getName() + ".lock");
        lockFile.deleteOnExit();

        return lockFile;
    }

    private LockFile getLock(File file) throws FileNotFoundException
    {
        LockFile lock = new LockFile(new RandomAccessFile(file, "rw").getChannel());
        lock.setAutoClose(true);

        return lock;
    }

    /**
     * @return the startingPrime
     */
    public int getStartingPrime()
    {
        return startingPrime;
    }

}
