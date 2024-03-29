package com.qlangtech.tis.hive;

import com.google.common.collect.Sets;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.IHiveMetaStore;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/20
 */
public class DefaultHiveMetaStore implements IHiveMetaStore {
    final IMetaStoreClient storeClient;
    private final String metaStoreUrls;
    private static final Logger logger = LoggerFactory.getLogger(DefaultHiveMetaStore.class);

    public DefaultHiveMetaStore(IMetaStoreClient storeClient, String metaStoreUrls) {
        this.storeClient = storeClient;
        this.metaStoreUrls = metaStoreUrls;
    }

    @Override
    public void dropTable(String database, String tableName) {
        try {
            storeClient.dropTable(database, tableName, true, true);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<HiveTable> getTables(String database) {
        try {
            // storeClient.getAllDatabases();
            // storeClient.createTable();
            List<String> tables = storeClient.getTables(database, ".*");
            return tables.stream().map((tab) -> new HiveTable(tab) {
                @Override
                public StoredAs getStoredAs() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<String> getPartitionKeys() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<String> listPartitions(Optional<String> filter) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public String getStorageLocation() {
                    throw new UnsupportedOperationException();
                }
            }).collect(Collectors.toList());
        } catch (TException e) {
            throw new RuntimeException("database:" + database, e);
        }
    }

    @Override
    public HiveTable getTable(String database, String tableName) {
        try {
            // storeClient.getta
            Table table = storeClient.getTable(database, tableName);

            StorageDescriptor storageDesc = table.getSd();


            return new HiveTable(table.getTableName()) {
                @Override
                public List<String> getPartitionKeys() {
                    return table.getPartitionKeys().stream().map((pt) -> pt.getName()).collect(Collectors.toList());
                }

                /**
                 * @see LazySimpleSerDe
                 * @return
                 */
                @Override
                public StoredAs getStoredAs() {
                    SerDeInfo serdeInfo = storageDesc.getSerdeInfo();
//                                            Map<String, String> params = serdeInfo.getParameters();
//                                            try {
//                                              if(Class.forName(serdeInfo.getSerializationLib()) == LazySimpleSerDe.class){
//                                                  params.get(serdeConstants.FIELD_DELIM);
//                                              }
//                                            } catch (ClassNotFoundException e) {
//                                                throw new RuntimeException(e);
//                                            }
                    return new StoredAs(storageDesc.getInputFormat(), storageDesc.getOutputFormat(), serdeInfo);
                }

                @Override
                public List<String> listPartitions(Optional<String> filter) {
                    try {
                        short maxPtsCount = (short) 999;
                        List<Partition> pts = null;
                        if (filter.isPresent()) {

                            String criteria = filter.get();
                            if (StringUtils.indexOf(criteria, HiveTable.KEY_PT_LATEST) > -1) {

                                criteria = DefaultHiveConnGetter.replaceLastestPtCriteria(criteria, (ptKey) -> {
                                    int index = 0;
                                    int matchedIndex = -1;
                                    for (FieldSchema pt : table.getPartitionKeys()) {
                                        if (StringUtils.equals(ptKey, pt.getName())) {
                                            matchedIndex = index;
                                            break;
                                        }
                                        index++;
                                    }

                                    if (matchedIndex < 0) {
                                        throw new IllegalStateException("has not find ptKey:" + ptKey + " in pt schema:" //
                                                + table.getPartitionKeys().stream().map((p) -> p.getName()).collect(Collectors.joining(",")));
                                    }
                                    Optional<String> maxPt = Optional.empty();
                                    Set<String> latestPts = Sets.newHashSet();
                                    try {
                                        for (Partition p : storeClient.listPartitions(database, tableName, maxPtsCount)) {
                                            latestPts.add(p.getValues().get(matchedIndex));
                                        }
                                        maxPt = latestPts.stream().max((pt1, pt2) -> {
                                            return pt1.compareTo(pt2);
                                        });
                                    } catch (TException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return maxPt.orElseThrow(() -> new IllegalStateException("can not find maxPt latestPts.size()=" + latestPts.size()));
                                });
                            }

                            pts = storeClient.listPartitionsByFilter(database, tableName, criteria, maxPtsCount);
                        } else {
                            pts = storeClient.listPartitions(database, tableName, maxPtsCount);
                        }
                        return pts.stream().map((pt) -> String.join(File.separator, pt.getValues())).collect(Collectors.toList());
                    } catch (TException e) {
                        throw new RuntimeException("table:" + tableName, e);
                    }
                }

                @Override
                public String getStorageLocation() {
                    return storageDesc.getLocation();
                }
            };
        } catch (NoSuchObjectException e) {
            logger.warn(database + "." + tableName + " is not exist in hive:" + metaStoreUrls);
            return null;
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        storeClient.close();
    }
}