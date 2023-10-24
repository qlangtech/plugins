//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.hadoop.fs;

public interface BlockStoragePolicySpi {
    String getName();

    StorageType[] getStorageTypes();

    StorageType[] getCreationFallbacks();

    StorageType[] getReplicationFallbacks();

    boolean isCopyOnCreateFile();
}
