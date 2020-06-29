# JindoFS SDK 使用

<a name="3baNh"></a>
# 介绍

JindoFS SDK 是一个简单易用面向 Hadoop/Spark 生态的 OSS 客户端，为阿里云 OSS 提供高度优化的 Hadoop FileSystem 实现。通过它您可以

1. 访问 OSS（作为 OSS 客户端）
1. 访问 JindoFS Cache 模式集群
1. 访问 JindoFS Block 模式集群



即使您使用JindoFS SDK 仅仅作为 OSS 客户端，相对于 Hadoop 社区 OSS 客户端实现，您还可以获得更好的性能和阿里云 E-MapReduce 产品技术团队更专业的支持。

目前支持的 Hadoop 版本包括 Hadoop 2.7+ 和 Hadoop 3.x。有问题请反馈，开 PR，我们会及时处理。<br />
<br />关于 JindoFS SDK 和 Hadoop 社区 OSS connector 的性能对比，请参考文档[JindoFS SDK 和Hadoop-OSS-SDK 性能对比测试](./jindofs_sdk_vs_hadoop_sdk.md)。<br />

<a name="CLFRq"></a>
# 使用方法

<a name="EKEBo"></a>
### 1. 安装jar包
下载最新的jar包 jindofs-sdk-x.x.x.jar ，将sdk包安装到hadoop的classpath下
```
cp ./jindofs-sdk-*.jar hadoop-2.8.5/share/hadoop/hdfs/lib/jindofs-sdk.jar
```
注意： 目前SDK只支持Linux、MacOS操作系统<br />

<a name="fewNY"></a>
### 2. 创建客户端配置文件
将下面环境变量添加到/etc/profile文件中<br />export B2SDK_CONF_DIR=/etc/jindofs-sdk-conf<br />创建文件 /etc/jindofs-sdk-conf/bigboot.cfg  包含以下主要内容
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


<a name="mZiaE"></a>
### 3. 使用 SDK
<a name="exCE9"></a>
#### 3.1 访问 OSS (cache 模式) 
```
hadoop fs -ls oss://<ak>:<secret>@<bucket>.<endpoint>/
```
您也可以将oss的ak、secret、endpoint预先配置在 hadoop-2.8.5/etc/hadoop/core-site.xml ，避免每次使用时临时填写ak。
```xml
<configuration>
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
然后就可以用以下方式访问OSS
```
hadoop fs -ls oss://<bucket>/
```
<a name="23Hbj"></a>
#### 3.2 访问 JindoFS 集群 (Block模式）
如果您部署了Block模式的JindoFS集群，或者开启Cache模式的缓存功能，需要额外配置
```
[bigboot-client]
client.storage.rpc.port=6101
client.namespace.rpc.address=header-1:8101 # the address of nameservice node
```
然后就可以用以下方式访问JindoFS集群
```
hadoop fs -ls jfs://<namespace>/
```
<a name="ko0uT"></a>
#### 3.3 在Java代码中使用 SDK
在maven中添加本地sdk jar包的依赖
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
然后您可以编写Java程序使用 SDK
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
注意，在IDE环境下，也要确保 B2SDK_CONF_DIR 环境变量已经设置。

<a name="WwYXi"></a>
# 发布日志

<a name="TqRR6"></a>
### v2.7.1
日期：20190619<br />文件：[jindofs-sdk-2.7.1.jar](https://smartdata-binary.oss-cn-shanghai.aliyuncs.com/jindofs-sdk-2.7.1.jar)<br />更新内容：

1. 支持访问 OSS （作为OSS客户端）
1. 支持访问 JindoFS Cache模式集群
1. 支持访问 JindoFS Block模式集群

<br />
