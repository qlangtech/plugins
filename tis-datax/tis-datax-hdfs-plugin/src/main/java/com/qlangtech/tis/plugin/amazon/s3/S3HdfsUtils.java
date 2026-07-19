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
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.qlangtech.tis.plugin.amazon.s3.S3FileSystem.SCHEMA_S3;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/3/8
 */
class S3HdfsUtils {
    private static final Logger logger = LoggerFactory.getLogger(S3HdfsUtils.class);
    private static final Map<String, FileSystem> fileSys = new HashMap<String, FileSystem>();
    private static final Lock lock = new ReentrantLock();
    private static final int LOCK_TIMEOUT_SECONDS = 10;
    private static final int FS_CREATION_TIMEOUT_SECONDS = 30;

    public static FileSystem getFileSystem(String s3Path, Configuration config) {

        FileSystem fileSystem = fileSys.get(s3Path);
        if (fileSystem == null) {
            boolean lockAcquired = false;
            try {
                lockAcquired = lock.tryLock(LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (!lockAcquired) {
                    throw new RuntimeException("Failed to acquire filesystem lock within " + LOCK_TIMEOUT_SECONDS
                            + " seconds, possible deadlock or long-running operation detected");
                }

                try {
                    fileSystem = fileSys.get(s3Path);
                    if (fileSystem == null) {
                        // 使用 ExecutorService 实现文件系统创建超时控制
                        ExecutorService executor = Executors.newSingleThreadExecutor();
                        try {
                            Future<FileSystem> future = executor.submit(() -> createFileSystem(s3Path, config));
                            fileSystem = future.get(FS_CREATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                            fileSys.put(s3Path, fileSystem);
                        } catch (TimeoutException e) {
                            throw new RuntimeException("Timeout creating S3 FileSystem after " + FS_CREATION_TIMEOUT_SECONDS
                                    + " seconds for path: " + s3Path, e);
                        } catch (ExecutionException e) {
                            Throwable cause = e.getCause();
                            if (cause instanceof TisException) {
                                throw (TisException) cause;
                            }
                            throw TisException.create("Failed to create S3 FileSystem for path: " + s3Path
                                    + ", detail: " + cause.getMessage(), cause);
                        } finally {
                            executor.shutdownNow();
                        }
                    }

                } catch (Throwable e) {
                    if (e instanceof TisException) {
                        throw (TisException) e;
                    }
                    throw TisException.create("Failed to get S3 FileSystem for path: " + s3Path
                            + ", detail: " + e.getMessage(), e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for filesystem lock", e);
            } finally {
                if (lockAcquired) {
                    lock.unlock();
                }
            }

        }
        return fileSystem;
    }


    private static FileSystem createFileSystem(String s3Path, Configuration config) throws IOException {
        config.set(FileSystem.FS_DEFAULT_NAME_KEY, s3Path);
        FileSystem fs = FileSystem.get(config);
        if (!SCHEMA_S3.equalsIgnoreCase(fs.getScheme())) {
            throw new IllegalStateException("fileSystem " + fs.getScheme() + "(" + fs.getClass().getName() + ") must be " + SCHEMA_S3);
        }
        FileSystem fileSystem = new FilterFileSystem(fs) {
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
                return super.create(f, FsPermission.getDefault(), overwrite, bufferSize, replication, blockSize, progress);
            }

            @Override
            public void close() throws IOException {
                // 设置不被关掉
            }
        };
        fileSystem.listStatus(new Path("/"));
        logger.info("successful create hdfs with hdfsAddress:" + s3Path);
        return fileSystem;
    }


}
