![BlobBase Logo](/docs/images/blobbase-logo.png)

# BlobBase
A Simple, Embeddable, Thread-Safe, Open Source, BLOB storage API.

BlobBase is a NoSQL database that stores/retrieves data from a managed directory structure on disk. Keys are mapped to file names located in the database that can then be opened, written to, and read from. It is intended to be used as a replacement for developers using a relational database to store image data. By using BlobBase instead of a traditional database, they can vastly decrease the sizes of their databases and improve performance and reliability.

## The chief advantages of this architecture are:

* Simplicity – each stored object is simply a file in the file system.
* Speed      – Key lookups and retrieval are very very fast even for extremely large databases.
* Robustness – since the database uses the file system it is extremely easy to manage and protected using RAID.

## Code Examples
**Write a Blob to database**
```java
 // location  of database
 File root = new File("myDb");
 // instantiate BlobBase
 BlobBase blobBase = BlobBase.getInstance(root);
 // create a file in database with the assigned key '123'
 File file = map.createFile(123);
 FileOutputStream out = new FileOutputStream(file);
/* do something with output stream */
```
**Read a Blob from database**
```java
 // location  of database
 File root = new File("myDb");
 // instantiate BlobBase
 BlobBase blobBase = BlobBase.getInstance(root);
 // get file from database using the key '123'
 File file = blobBase.getFile(123);
 FileInputStream in = new FileInputStream(file);
/* do something with input stream */
```
**Delete Blob from database**
```java
 // location  of database
 File root = new File("myDb");
 // instantiate BlobBase
 BlobBase blobBase = BlobBase.getInstance(root);
 // delete blob with key '123'
 boolean bdeleted = blobBase.deleteFile(123);        
 assertTrue(bdeleted);
```

## BlobBase IO Routines

BlobBaseInputStream and BlobBaseOutputStream are two convenience methods for reading and writing to the database.

**BlobBaseOutputStream**
```java
 // location  of database
 File root = new File("myDb");
 // instantiate BlobBase
 BlobBase blobBase = BlobBase.getInstance(root);
 // get BlobBaseOutputStream for key '123'
 BlobBaseOutputStream os = new BlobBaseOutputStream(blobBase,123);
 os.write("hello world".getBytes());
```

**BlobBaseInputStream**
```java
 // location  of database
 File root = new File("myDb");
 // instantiate BlobBase
 BlobBase blobBase = BlobBase.getInstance(root);
 // get BlobBaseInputStream for key '123'
 BlobBaseInputStream is = new BlobBaseInputStream(blobBase,123);
 while (true)
 {
   int b = is.read();
   if (b == -1)
   {
     break;
   }
   // do something with byte read 
 }
```
**Compression**
BlobBase can automaticaaly compress your data When using BlobBaseInputStream and BlobBaseOutputStream. To enable this compression do the following:
```java
// set location  of database
File root = new File("./myDb");

// instantiate BlobBase
BlobBase blobBase = BlobBase.getInstance(root);
// turn compression on
blobBase.setCompressed();
```
## Setting Attributes ##
You can attach attributes to be stored with you Blob files. This can be a handy way to store those extra bits of information we always seem to need.
```java
// set location  of database
File root = new File("./myDb");

// instantiate BlobBase
BlobBase blobBase = BlobBase.getInstance(root);
Map<String,Object> attrMap = map.getAttributeMap(123); // get attributes for object 123
attrMap.put("testAttr", "hello world!");               // set our attribute
map.setAttributeMap(123, attrMap);                     // and update database
```


## Builing Sources
This SDK uses maven as its build system. Running mvn install will build the system.
```bash
$ mvn install
```
Their are tests that can be run as well (may require editing of database path). To run these
```bash
$ mvn test -DskipTests=false
```
