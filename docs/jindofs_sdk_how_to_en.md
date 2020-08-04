# JindoFS SDK User Guide
[中文版](./jindofs_sdk_how_to.md)

<a name="V3xjc"></a>
# Indroduce
JindoFS SDK is an easy-to-use OSS client for Hadoop/Spark ecosystem, providing highly optimized Hadoop FileSystem implementation for Ali Cloud OSS. Through it you can do the following operations:

1. Access to OSS (as an OSS client)
1. Access to JindoFS Cache mode cluster
1. Access to JindoFS Block mode cluster


<br />Even if you use JindoFS SDK only as an OSS client, you can get better performance and more professional support from the Aliyun E-MapReduce technical team than the OSS client implementation of Hadoop community.<br />
<br />Currently JindoFS SDK supported Hadoop 2.7+ and Hadoop 3.x versions. If you have any questions, please give feedback. Open PR, and we will deal with it in time.<br />
<br />For a performance comparison between the JindoFS SDK and the Hadoop Community OSS Connector, refer to the documentation [Performance Comparison of JindoFS SDK and Hadoop-OSS-SDK](./jindofs_sdk_vs_hadoop_sdk_en.md).<br />

<a name="7QUTJ"></a>
# Steps
<a name="dWKjA"></a>
### 1. Deploy JindoFS SDK jar
Download the latest JindoFS SDK jar package jindofs-sdk-x.x.x.jar and install the SDK package under the Hadoop classpath.
```
cp ./jindofs-sdk-*.jar hadoop-2.8.5/share/hadoop/hdfs/lib/jindofs-sdk.jar
```

Note： currently, JindoFS SDK only supports Linux and MacOS operating systems.<br />

<a name="WHNoU"></a>
### 2. Create a client configuration file
Add the following environment variable to /etc/profile
```
export B2SDK_CONF_DIR=/etc/jindofs-sdk-conf
```

Create a file /etc/jindofs-sdk-conf/bigboot.cfg with following contents
```
[bigboot]
logger.dir = /tmp/bigboot-log
[bigboot-client]
client.oss.retry=5
client.oss.upload.threads=4
client.oss.upload.queue.size=5
client.oss.upload.max.parallelism=16
client.oss.timeout.millisecond=30000
client.oss.connection.timeout.millisecond=3000
```

<a name="itFcV"></a>
### 3. Use JindoFS SDK
<a name="exCE9"></a>
#### 3.1 Access to OSS (Cache Mode) 
```
hadoop fs -ls oss://<ak>:<secret>@<bucket>.<endpoint>/
```

You can also pre-configure ak, secret and endpoint of OSS to hadoop-2.8.5/etc/hadoop-core-site.xml to avoid filling in these each time you use it.
```xml
<configuration>
    <property>
        <name>fs.AbstractFileSystem.oss.impl</name>
        <value>com.aliyun.emr.fs.oss.OSS</value>
    </property>

    <property>
        <name>fs.oss.impl</name>
        <value>com.aliyun.emr.fs.oss.JindoOssFileSystem</value>
    </property>

    <property>
        <name>fs.jfs.cache.oss-accessKeyId</name>
        <value>xxx</value>
    </property>

    <property>
        <name>fs.jfs.cache.oss-accessKeySecret</name>
        <value>xxx</value>
    </property>

    <property>
        <name>fs.jfs.cache.oss-endpoint</name>
        <value>oss-cn-xxx.aliyuncs.com</value>
    </property>

  	<property>
  			<name>fs.jfs.cache.copy.simple.max.byte</name>
  			<value>67108864</value>
      	<description>set to -1 if your oss bucket supports shallow copy.</description>
		</property>
</configuration>
```

Then OSS can then be accessed in the following way:
```
hadoop fs -ls oss://<bucket>/
```

<a name="23Hbj"></a>
#### 3.2 Access to JindoFS Cluster (Block Mode）
Additional configurations are required if you deploy a block mode JindoFS cluster, or if caching is enabled in cache mode.
```
[bigboot-client]
client.storage.rpc.port=6101
client.namespace.rpc.address=header-1:8101 # the address of nameservice node
```

Then JindoFS cluster can then be accessed in the following way:
```
hadoop fs -ls jfs://<namespace>/
```

<a name="ko0uT"></a>
#### 3.3 Using SDK in Java Code
Add local JindoFS SDK jars to maven dependencies.
```xml
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.8.5</version>
        </dependency>
        <dependency>
            <groupId>bigboot</groupId>
            <artifactId>jindofs</artifactId>
            <version>0.0.1</version>
            <scope>system</scope>
            <systemPath>/Users/xx/xx/jindofs-sdk-2.7.1.jar</systemPath>
        </dependency>
```

Then you can write a Java program using the JindoFS SDK.
```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class TestJindoSDK {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create("oss://<bucket>/"), conf);
    FSDataInputStream in = fs.open(new Path("/uttest/file1"));
    in.read();
    in.close();
  }
}
```

Note: make sure the B2SDK_CONF_DIR environment variable is set in an IDE environment.<br />

<a name="WwYXi"></a>
# Release Notes

<a name="TqRR6"></a>
### v2.7.1
Date：20190619<br />File：[jindofs-sdk-2.7.1.jar](https://smartdata-binary.oss-cn-shanghai.aliyuncs.com/jindofs-sdk-2.7.1.jar)<br />Major Features：

1. Supports access to OSS (as an OSS client)
1. Supports access to JindoFS Cache mode cluster
1. Supports access to JindoFS Block mode cluster



