package com.qlangtech.tis.plugin.amazon.s3;

import com.qlangtech.tis.lang.TisException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.qlangtech.tis.plugin.amazon.s3.S3FileSystem.SCHEMA_S3;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/3/8
 */
class S3HdfsUtils {
    private static final Logger logger = LoggerFactory.getLogger(S3HdfsUtils.class);
    private static final Map<String, FileSystem> fileSys = new HashMap<String, FileSystem>();

    public static FileSystem getFileSystem(String s3Path, Configuration config) {

        FileSystem fileSystem = fileSys.get(s3Path);
        if (fileSystem == null) {
            synchronized (S3HdfsUtils.class) {
                try {
                    fileSystem = fileSys.get(s3Path);
                    if (fileSystem == null) {
//                            Configuration conf = new Configuration();
//                            conf.set(FsPermission.UMASK_LABEL, "000");
//                            // fs.defaultFS
//                            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

//                            //https://segmentfault.com/q/1010000008473574
//                            Logger.info("userHostname:{}", userHostname);
//                            if (userHostname != null && userHostname) {
//                                conf.set("dfs.client.use.datanode.hostname", "true");
//                            }
//
//                            conf.set("fs.default.name", hdfsAddress);
//                            conf.set("hadoop.job.ugi", "admin");
//                            try (InputStream input = new ByteArrayInputStream(hdfsContent.getBytes(TisUTF8.get()))) {
//                                conf.addResource(input);
//                                // 这个缓存还是需要的，不然如果另外的调用FileSystem实例不是通过调用getFileSystem这个方法的进入,就调用不到了
//                                conf.setBoolean("fs.hdfs.impl.disable.cache", false);
                        config.set(FileSystem.FS_DEFAULT_NAME_KEY, s3Path);
                        FileSystem fs = FileSystem.get(config);
                        if (!SCHEMA_S3.equalsIgnoreCase(fs.getScheme())) {
                            throw new IllegalStateException("fileSystem " + fs.getScheme() + "(" + fs.getClass().getName() + ") must be " + SCHEMA_S3);
                        }
                        fileSystem = new FilterFileSystem(fs) {
                            @Override
                            public boolean delete(Path f, boolean recursive) throws IOException {
                                try {
                                    return super.delete(f, recursive);
                                } catch (Exception e) {
                                    throw new RuntimeException("path:" + f, e);
                                }
                            }

                            @Override
                            public boolean mkdirs(Path f, FsPermission permission) throws IOException {
                                return super.mkdirs(f, FsPermission.getDirDefault());
                            }

                            @Override
                            public FSDataOutputStream create(Path f, FsPermission permission
                                    , boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {

//                                    if (f.getName().indexOf("hoodie.properties") > -1) {
//                                        throw new IllegalStateException("not support:" + f.getName());
//                                    }

                                return super.create(f, FsPermission.getDefault(), overwrite, bufferSize, replication, blockSize, progress);
                            }

//                                @Override
//                                public FileStatus[] listStatus(Path f) throws IOException {
//                                    return super.listStatus(f);
//                                }

                            @Override
                            public void close() throws IOException {
                                // super.close();
                                // 设置不被关掉
                            }
                        };
                        fileSystem.listStatus(new Path("/"));
                        logger.info("successful create hdfs with hdfsAddress:" + s3Path);
                        fileSys.put(s3Path, fileSystem);
                    }

                } catch (Throwable e) {
                    // throw new RuntimeException("link faild:" + hdfsAddress, e);
                    throw TisException.create("link faild:" + s3Path + ",detail:" + e.getMessage(), e);
                } finally {

                }
            }

        }
        return fileSystem;
    }


}
