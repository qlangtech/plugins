package com.qlangtech.tis.plugin.amazon.s3;

import com.qlangtech.tis.fs.IPath;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/3/8
 */
public class AmazonS3Path implements IPath {

    private final Path path;

    public AmazonS3Path(String path) {
        this(new Path(path));
    }

    public AmazonS3Path(Path path) {
        this.path = path;
    }
    public AmazonS3Path(IPath parent, String name) {
        this.path = new Path(parent.unwrap(Path.class), name);
    }
    @Override
    public String toString() {
        return this.path.toString();
    }

    /**
     * 取得文件名称
     *
     * @return
     */
    @Override
    public String getName() {
        return path.getName();
    }



    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz != Path.class) {
            throw new IllegalStateException(clazz + " is not type of " + Path.class);
        }
        return clazz.cast(this.path);
    }
}