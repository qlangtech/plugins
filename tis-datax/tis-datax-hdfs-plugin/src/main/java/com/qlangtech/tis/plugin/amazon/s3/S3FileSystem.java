package com.qlangtech.tis.plugin.amazon.s3;

import com.qlangtech.tis.fs.FSDataInputStream;
import com.qlangtech.tis.fs.IContentSummary;
import com.qlangtech.tis.fs.IFileSplitor;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.IPathInfo;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.TISFSDataOutputStream;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.List;

/**
 * HdfsFileSystem
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/3/8
 */
public class S3FileSystem implements ITISFileSystem {

    public static final String SCHEMA_S3 = "s3a";

    private final FileSystem fs;
    private final String s3Path;
    private final String rootDir;

    public S3FileSystem(FileSystem fs, String s3Path, String rootDir) {
        this.fs = fs;
        this.s3Path = s3Path;
        this.rootDir = rootDir;
    }

    @Override
    public IPath getPath(String path) {
        if (StringUtils.startsWith(path, "/")) {
            return new AmazonS3Path(path);
        } else {
            // 相对目录
            return new AmazonS3Path(getRootDir(), path);
        }

    }


    @Override
    public IPath getPath(IPath parent, String name) {
        return new AmazonS3Path(IPath.pathConcat(parent.toString(), name));
    }

    @Override
    public IPath getRootDir() {
        return new AmazonS3Path(this.s3Path);
    }

    @Override
    public String getName() {
        return IEndTypeGetter.EndType.AmazonS3.getVal();
    }


    @Override
    public OutputStream getOutputStream(IPath p) {
        try {
            Path path = p.unwrap(Path.class);
            Configuration conf = fs.getConf();
            FsServerDefaults fsDefaults = fs.getServerDefaults(path);
            EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
//            if (Boolean.getBoolean(HDFS_SYNC_BLOCK)) {
//                flags.add(CreateFlag.SYNC_BLOCK);
//            }
            return fs.create(path, FsPermission.getDefault().applyUMask(FsPermission.getUMask(conf)), flags, fsDefaults.getFileBufferSize(), fsDefaults.getReplication(), fsDefaults.getBlockSize(), null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FSDataInputStream open(IPath path, int bufferSize) {
        try {
            org.apache.hadoop.fs.FSDataInputStream input = fs.open(this.unwrap(path), bufferSize);
            return new DefaultFSDataInputStream(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path unwrap(IPath path) {
        return path.unwrap(Path.class);
    }

    private static class DefaultFSDataInputStream extends FSDataInputStream {

        public DefaultFSDataInputStream(org.apache.hadoop.fs.FSDataInputStream in) {
            super(in);
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            ((org.apache.hadoop.fs.FSDataInputStream) this.in).readFully(position, buffer, offset, length);
        }

        @Override
        public void seek(long position) {
            //try {
            ((FSDataInputStream) this.in).seek(position);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
        }
    }

    @Override
    public IContentSummary getContentSummary(IPath path) {
        try {
            final ContentSummary summary = fs.getContentSummary(this.unwrap(path));
            return new IContentSummary() {

                @Override
                public long getLength() {
                    return summary.getLength();
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IPathInfo getFileInfo(IPath path) {
        try {
            FileStatus status = fs.getFileStatus(this.unwrap(path));
            return new DefaultIPathInfo(status);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private class DefaultIPathInfo implements IPathInfo {

        private final FileStatus stat;

        public DefaultIPathInfo(FileStatus stat) {
            super();
            this.stat = stat;
        }

        @Override
        public String getName() {
            return stat.getPath().getName();
        }

        @Override
        public IPath getPath() {
            return S3FileSystem.this.getPath(stat.getPath().toString());
        }

        @Override
        public boolean isDir() {
            return stat.isDirectory();
        }

        @Override
        public long getModificationTime() {
            return stat.getModificationTime();
        }

        @Override
        public long getLength() {
            return stat.getLen();
        }
    }

    @Override
    public boolean delete(IPath f, boolean recursive) throws IOException {
        return fs.delete(this.unwrap(f), recursive);
    }


    @Override
    public FSDataInputStream open(IPath path) {
        return null;
    }

    @Override
    public TISFSDataOutputStream create(IPath f, boolean overwrite, int bufferSize) throws IOException {
        return null;
    }

    @Override
    public TISFSDataOutputStream create(IPath f, boolean overwrite) throws IOException {
        return null;
    }

    @Override
    public boolean exists(IPath path) {
        return false;
    }

    @Override
    public boolean mkdirs(IPath f) throws IOException {
        return false;
    }

    @Override
    public void copyToLocalFile(IPath srcPath, File dstPath) {

    }

    @Override
    public void rename(IPath from, IPath to) {

    }

    @Override
    public boolean copyFromLocalFile(File localIncrPath, IPath remoteIncrPath) {
        return false;
    }

    @Override
    public IFileSplitor getSplitor(IPath path) throws Exception {
        return null;
    }


    @Override
    public List<IPathInfo> listChildren(IPath path) {
        return List.of();
    }

    @Override
    public List<IPathInfo> listChildren(IPath path, IPathFilter filter) {
        return List.of();
    }


    @Override
    public boolean delete(IPath f) throws IOException {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public <T> T unwrap() {
        return (T) fs;
    }
}
