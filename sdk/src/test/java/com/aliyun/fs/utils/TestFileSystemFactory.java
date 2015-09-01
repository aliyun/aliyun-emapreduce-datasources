package com.aliyun.fs.utils;

import com.aliyun.fs.oss.blk.OssFileSystem;
import com.aliyun.fs.oss.nat.NativeOssFileSystem;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class TestFileSystemFactory extends TestCase {

    private Path path1 = new Path("oss://bucket/test1");
    private Path path2 = new Path("ossn://bucket/test2");
    private Path path3 = new Path("hdfs:///test3");
    private Path path4 = new Path("file://test4");

    public void testFileSystemCreate() throws IOException {
        Configuration conf = new Configuration();

        assert(FileSystemFactory.get(path1, conf) instanceof OssFileSystem);
        assert(FileSystemFactory.get(path2, conf) instanceof NativeOssFileSystem);
    }

    public void testCheckBlockBased() {
        assert(FileSystemFactory.checkBlockBased(path1));
        assert(!FileSystemFactory.checkBlockBased(path2));
        assert(!FileSystemFactory.checkBlockBased(path3));
        assert(!FileSystemFactory.checkBlockBased(path4));
    }
}
