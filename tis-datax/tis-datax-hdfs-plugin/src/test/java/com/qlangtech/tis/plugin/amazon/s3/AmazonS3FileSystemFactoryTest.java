package com.qlangtech.tis.plugin.amazon.s3;

import com.qlangtech.tis.config.authtoken.impl.DefaultHiveUserToken;
import com.qlangtech.tis.fs.IPathInfo;
import com.qlangtech.tis.fs.ITISFileSystem;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/3/8
 */
public class AmazonS3FileSystemFactoryTest {

    @Test
    public void testCreateFileSystem() {
        AmazonS3FileSystemFactory s3FSFactory = new AmazonS3FileSystemFactory();
        s3FSFactory.bucket = "tis";
        s3FSFactory.endpoint = "http://192.168.28.200:9000";
       // s3FSFactory.userHostname = false;
        s3FSFactory.rootDir = "/test";
        DefaultHiveUserToken token = new DefaultHiveUserToken();
        token.userName = "baisui";
        token.password = "12345678";
        s3FSFactory.userToken = token;


        ITISFileSystem fileSystem = s3FSFactory.getFileSystem();
        Assert.assertNotNull(fileSystem);

        IPathInfo fileInfo = fileSystem.getFileInfo(fileSystem.getPath("/README.txt"));
        Assert.assertNotNull(fileInfo);
    }
}
