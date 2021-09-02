# MATLAB&reg; interface for WebHDFS

## Table of Contents

- [Introduction](#Introduction)
  * [What is WebHDFS?](#What-is-WebHDFS)
  * [The MATLAB&reg; interface for WebHDFS](#The-webhdfs-interface-for-MATLAB)
  * [When should you use WebHDFS?](#When-should-you-use-webhdfs)
- [Methodology](#Installing-or-Updating-the-toolbox)
  * [Requirements](#Requirements)
  * [Installation](#Installation)
  * [Future Updates](#Future-Updates)
- [License](#Licence)
- [Getting Started](#Getting-Started)
  * [Setting up the connection](#Setting-up-the-connection)
  * [Authentication mechanisms](#Authentication-mechanisms)
  * [Listing files and folders](#Listing-files-and-folders)
  * [Creating and deleting directories](#Creating-and-deleting-directories)
  * [Downloading and Uploading files and folders](#Downloading-and-Uploading-files-and-folders)
  * [Reading and Writing files directly on Hadoop&reg;](#Reading-and-Writing-files-directly-on-Hadoop&reg;)
  * [Changing file permissions](#Changing-file-permissions)
- [Enhancement requests](#Enhancement-requests)

## Introduction

### What is WebHDFS?

WebHDFS is a protocol that defines a public HTTP REST API which permits clients to access Hadoop&reg; Distributed File System (HDFS) over the Web. It retains the security the native Hadoop&reg; protocol offers and uses parallelism, for better throughput. To use this toolbox, the webhdfs functionality needs to be enabled in the Hadoop&reg; Server.

### The MATLAB&reg; interface for WebHDFS

This toolbox provides a set of functions that enable the user to directly work with files and folders stored in Hadoop&reg; via a [REST API](https://hadoop.apache.org/docs/r3.3.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) and perform common operations such as read, write, upload, and download files.

### When should you use WebHDFS?

When working with Hadoop&reg; files, the WebHDFS is not the only alternative and you might want to consider other alternatives depending on the task at hand.

- For Big Data applications, you can prototype an algorithm in MATLAB&reg; either using [tall arrays](https://www.mathworks.com/help/compiler/spark/example-on-deploying-tall-arrays-to-a-spark-enabled-hadoop-cluster.html) or our [Spark API](https://www.mathworks.com/help/compiler/spark/example-on-deploying-applications-to-spark-using-the-matlab-api-for-spark.html) and [deploy them direclty on Spark enabled Hadoop&reg; cluster](https://www.mathworks.com/help/compiler/deploy-applications-using-the-matlab-api-for-spark.html)

- You can access your files using Hive and Impala, and run any SQL or HQL command. This tool might be more suitable to run queries on large pieces of data.

These tools might be more suitable to run analytics on large sets of data, while the webhdfs interface might be a better tool to do small operations, since the data needs to travel back and forth over the internet.

## Installing or Updating the toolbox

### Requirements

1. Only base [MATLAB&reg;](https://www.mathworks.com/products/matlab.html) is required to run all the toolbox functionality. For users accessing Hadoop&reg; via Kerberos authentication **R2019b** or newer is recommended.

2. [optional] [Database Toolbox&trade;](https://www.mathworks.com/products/database.html) is needed to run Hive and Impala

3. [optional]  [MATLAB&reg; Compiler&trade;](https://www.mathworks.com/products/compiler.html) is needed to deploy Spark or mapreduce jobs

4. [optional] [MATLAB&reg; Parallel Server&trade;](https://www.mathworks.com/products/matlab-parallel-server.html) is needed to run interactive Spark jobs

### Installation

The toolbox can be installed directly from the Add-On explorer, or by double-clicking the `mltbx` file. All the functionality will be then accessible under the namepsace `webhdfs.*`

### Future updates

The toolbox will be updated regularly. To get the newest version, you can simply uninstall and re-install the toolbox direclty from the Add-On explorer in MATLAB.

## Licence

This toolbox is licensed under an XLSA license. Please see [LICENSE.txt](LICENSE.txt).

## Getting Started

A complete documentation for the toolbox can be found in the [getting started guide](tbx/doc/GettingStarted.mlx). However, the most common tasks are also outlined below.

For more information, please look at the documentation of the toolbox:

```matlab:Code
doc WebHdfsClient
```

### Setting up the connection

The connection to a Hadoop&reg; cluster is always done via the class `WebHdfsClient`. This class supports several optional arguments:

   -  root [optional]: Root folder to parent all requests. If unspecified, all requests are assumed to be relative to "\".
   -  protocol [optional]: whether the connection is done via http or https (default). 
   -  host [optional]: hostname of the server running Hadoop&reg;
   -  port [optional]: port number where Hadoop&reg; is running 
   -  name [optional]: for unauthenticated servers only. Specify the name of the user.

```matlab:Code
client = WebHdfsClient(root = 'data', protocol = 'http', host = "sandbox-hdp.hortonworks.com", port = 50070);
```

To avoid having to set the connection details every time, you can save the connection details so future connections only require the root folder of your requests (if any). <u>These preferences persist between MATLAB&reg; sessions</u>

```matlab:Code
client.saveConnectionDetails();
client = WebHdfsClient("root", 'data');
```
These saved preferences can be removed at any poiny by running:

```matlab:Code(Display)
client.clearConnectionDetails();
```
When you work with files and folders you can specify a relative or absolute path. If the specified path starts with "/", it will be interpreted as an absolute path. Otherwise, the code will interpret the path as relative to the "root" folder in the server. For example, the following line lists the status of the folder `"/data/myData/testMW"`

```matlab:Code
client = WebHdfsClient("root", 'data');
status = client.hdfs_status("myData/testMW")
```

```text:Output
status = 
          accessTime: 0
           blockSize: 0
         childrenNum: 0
              fileId: 2495973
               group: 'hdfs'
              length: 0
    modificationTime: 1.6262e+12
               owner: 'maria_dev'
          pathSuffix: ''
          permission: '755'
         replication: 0
       storagePolicy: 0
                type: 'DIRECTORY'

```
 
### Authentication mechanisms

There are two authentication mechanisms supported by the toolbox:

1. For unauthenticated servers, you will need to specify your Username during the hdfs connection if you want to access any user specific operations.

```matlab:Code
client = WebHdfsClient(root = 'data', name = "maria_dev");
```
2. For Kerberos authentication, please use **R2019b** or newer. The authentication will be done automatically.
 
### Exploring the Hadoop&reg; file system

The following section outlines the most common commands to work with HDFS files. It shows how one can navigate the directories, open, download, and upload new files direclty in HDFS.

#### Listing files and folders

The methods `hdfs_list` and `hdfs_content` will give you information about the files inside a specific folder. For example, the following command returns the names and status of all the files within the folder `/data/myData`

```matlab:Code
client = WebHdfsClient("root", 'data');
elements = client.hdfs_list("myData", status = true)
```

|Fields|accessTime|blockSize|childrenNum|fileId|group|length|modificationTime|owner|pathSuffix|permission|replication|storagePolicy|type|
|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
|1|1.6262e+12|134217728|0|2499596|'hdfs'|184|1.6262e+12|'maria_dev'|'petdata.csv_...|'777'|1|0|'FILE'|
|2|1.6238e+12|134217728|0|2472035|'hdfs'|184|1.6238e+12|'maria_dev'|'petdata2.csv...|'777'|1|0|'FILE'|
|3|0|0|0|2495973|'hdfs'|0|1.6262e+12|'maria_dev'|'testMW'|'755'|0|0|'DIRECTORY'|

Alternatively, if `status` is set to `false`, only the names of the files are returned.

Similarly, the method `hdfs_recent_files`, allows you to find most recently modified files in a directory. By default function returns one file but you can specify the maximum number of files return with the nfiles argument. If you you set the nfiles argument to None, then you will get back list of all files. This function returns only the file names. For example, to view the latest file added to the folder `/data/myData`, we can run:

```matlab:Code
files = client.hdfs_recent_files("myData", 1)
```

```text:Output
files = 
          accessTime: 1.6262e+12
           blockSize: 134217728
         childrenNum: 0
              fileId: 2499552
               group: 'hdfs'
              length: 184
    modificationTime: 1.6262e+12
               owner: 'maria_dev'
          pathSuffix: 'petdata.csv'
          permission: '777'
         replication: 1
       storagePolicy: 0
                type: 'FILE'

```

#### Creating, moving, and deleting directories
 
You can use the method `hdfs_makedirs` to create Hadoop&reg; directories. It will recursively create intermediate directories if they are missing. For example, the following call:

```matlab:Code(Display)
client.hdfs_makedirs("one/two/three");
```

would also create directory `one/two` if they were missing. Additionally, this method accepts an optional `overwrite` parameter (true/false) to specify if the folder needs to be overwritten. **Pleaes note that all contents will be discarded if overwrite is set to true.**

You can delete files and directories from Hadoop&reg; with the `hdfs_delete` method. Files/directories are not moved to the HDFS Trash so they will be **permanently deleted**. 

`hdfs_delete` will return True if the file/directory was deleted and False if the file/directory did not exist. 

```matlab:Code(Display)
client.hdfs_delete("one/two/three")
```

By default non-empty directories will not be deleted. However if you set the optional recursive argument to True then files/directories will be deleted recursively.

```matlab:Code(Display)
client.hdfs_delete("one", recursive=true)
```

Finally, you can move files/directories in Hadoop&reg; with the `hdfs_rename` method:

   -  If the destination is an existing directory, then the source file/directory will be moved into it. 
   -  If the destination is an existing file, then this method will return false. 
   -  If the parent destination is missing, then this method will return false. 

```matlab:Code(Display)
client.hdfs_rename("share/one/two", "share/one/four")
```
 
#### Downloading and Uploading files and folders

With `hdfs_download` you can download files from a Hadoop&reg; directory. For exmaple, to download something into a temporary directory you can run:

```matlab:Code
client.hdfs_download('<hdfs_path>', tempdir());
```

If `hdfs_path` is a file then that file will be downloaded. If the argument is a directory then all the files and subfolders (together with their files) in that directory will be downloaded. 

Note that wildcards are not supported so you can either download complete contents of a directory or individual files. If the local file or directory already exists then it will not be overwritten and an error will be raised. However, you can set an overwrite flag to force the download of the files:

```matlab:Code(Display)
client.hdfs_download(testFileName, tempdir(), overwrite=true);
```

The same process is equivalent to uploading files in HDFS. You can upload local files and folders with the `hdfs_upload` method:

   -  If the target HDFS path exists and is a directory, then the files will be uploaded into it. 
   -  If the target HDFS path exists and is a file, then it will be overwritten if the optional overwrite argument is set to True. 

For example, to upload a single file, with the chosen permissions you can run:

```matlab:Code(Display)
lab.hdfs_upload("/data/one", "myfolder", overwrite = true, permission = 777)
```

#### Reading and Writing files directly on Hadoop&reg;

With `hdfs_open`, `hdfs_read`, and `hdfs_write` you can directly read data from files in Hadoop&reg; folders. The files will be read in memory, so you will not create a local copy of the file. Use the standard MATLAB&reg; modes "r" for text files and "rb" for binary files like parquet. For example:

```matlab:Code
testFileName = 'myData/matlab_WebHdfsPetdata.csv';
reader = client.hdfs_open(testFileName,'r')
```

```text:Output
reader = 
  WebHdfsFile with properties:

     encoding: "utf-8"
    hdfs_path: "/data/myData/Petdata.csv"
         mode: 'r'

```

```matlab:Code
data = reader.read();
disp(data)
```

```text:Output
Row,Age,Weight,Height
Sanchez,38,176,71
Johnson,43,163,69
Lee,38,131,64
Diaz,40,133,67
Brown,49,119,64
Sanchez,38,176,71
Johnson,43,163,69
Lee,38,131,64
Diaz,40,133,67
Brown,49,119,64
```

If the data can be read using a standard MATLAB&reg; command such as `readtable`, `imread`, or `parquetread`, you can pass this command (with its standard inputs or parameters) as:

```matlab:Code
data = reader.read(@(x) readtable(x,'ReadVariableNames',false))
```

| |Var1|Var2|Var3|Var4|
|:--:|:--:|:--:|:--:|:--:|
|1|'Sanchez'|38|176|71|
|2|'Johnson'|43|163|69|
|3|'Lee'|38|131|64|
|4|'Diaz'|40|133|67|
|5|'Brown'|49|119|64|
|6|'Sanchez'|38|176|71|
|7|'Johnson'|43|163|69|
|8|'Lee'|38|131|64|
|9|'Diaz'|40|133|67|
|10|'Brown'|49|119|64|

With `hdfs_open` function you can also write data to files in HDFS. Note that this is different from uploading files, as the file will not exist in a local path, it will be created from the data in memory.

You can overwrite existing files by setting the mode to "wb" (binary) or "wt" (text), and you can append to an existing files by setting the mode to "at" (text) or "ab" (binary). Note that appending is supported with text files and some binary formats like Avro. Appending is not supported with Parquet files.

The toolbox has four helper methods to help you write data into the file depending on the format that you choose to write. 

   -  "write": to add any type of text data to the file. 
   -  "writeTable": to add/append tables to a file. 
   -  "writeCell": to add/append cells to a file. 
   -  "writeMatrix": to add/append matrices to a file. 

```matlab:Code
testFileName = '/data/myData/petdata.csv';

file = client.hdfs_open(testFileName, 'w+');
writeTable(file, T)
file.read(@readtable)
```

| |Age|Weight|Height|
|:--:|:--:|:--:|:--:|
|1|38|176|71|
|2|43|163|69|
|3|38|131|64|
|4|40|133|67|
|5|49|119|64|

By default, the funciton writeTable in "write" mode, will only add the variable names to the table in UTF-8 format. This features can also be set as follows:

```matlab:Code
writeTable(file, T, 'WriteVariableNames', false, 'WriteRowNames', true, 'encoding', 'UTF-8')
file.read(@(x) readtable(x,'ReadRowNames', true))
```

| |Var1|Var2|Var3|
|:--:|:--:|:--:|:--:|
|1 Sanchez|38|176|71|
|2 Johnson|43|163|69|
|3 Lee|38|131|64|
|4 Diaz|40|133|67|
|5 Brown|49|119|64|


```matlab:Code
close(file);
```

Finally, if the file is opened in "append" mode, the data will be added at the end of the file. If the file did not exist, an empty file would be created upon start.

```matlab:Code
testFileName = 'myData/petdata.csv';
file = client.hdfs_open(testFileName, 'a+');
```

Unlike write mode, the default settings in append mode do not add variable headers, or row names to the file. You can, however, specify these same options in append mode.

```matlab:Code
file.writeTable(T,'WriteVariableNames', true, 'WriteRowNames', true)
file.writeTable(T, 'WriteRowNames', true)
file.read(@(x) readtable(x,'ReadRowNames', true))
```

| |Age|Weight|Height|
|:--:|:--:|:--:|:--:|
|1 Sanchez|38|176|71|
|2 Johnson|43|163|69|
|3 Lee|38|131|64|
|4 Diaz|40|133|67|
|5 Brown|49|119|64|
|6 Sanchez_1|38|176|71|
|7 Johnson_1|43|163|69|
|8 Lee_1|38|131|64|
|9 Diaz_1|40|133|67|
|10 Brown_1|49|119|64|

#### Changing file permissions

It is possible to set the read/write/execute file permissions programmatically. You need to pass the file permissions as a 3-number octal as show here [Chmod Calculator (chmod-calculator.com)](https://chmod-calculator.com/)

```matlab:Code
client.hdfs_set_permission( 'myData/petdata.csv', 777);
```

## Enhancement requests

To report any bug, or enchancement request, pelase submit a GitHub issue.

