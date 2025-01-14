package com.qlangtech.tis.hive;

import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.HiveTable.HiveTabColType;
import com.qlangtech.tis.config.hive.meta.HiveTable.StoredAs;
import com.qlangtech.tis.config.hive.meta.IHiveMetaStore;
import com.qlangtech.tis.config.hive.meta.PartitionFilter;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
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
                public List<HiveTabColType> getCols() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<String> getPartitionKeys() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<String> listPaths(PartitionFilter filter) {
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

    private static class HiveStoredAs extends StoredAs {
        private final SerDeInfo serdeInfo;

        public HiveStoredAs(StorageDescriptor storageDesc) {
            super(storageDesc.getInputFormat(), storageDesc.getOutputFormat());
            this.serdeInfo = Objects.requireNonNull(storageDesc.getSerdeInfo(), "serdeInfo");
        }

        @Override
        public Properties getSerdeProperties(HiveTable table) {
            Map<String, String> sdParams = serdeInfo.getParameters();
            Properties props = new Properties();
            for (Map.Entry<String, String> entry : sdParams.entrySet()) {
                props.setProperty(entry.getKey(), entry.getValue());
            }
            List<HiveTabColType> cols = table.getCols();
            props.setProperty(serdeConstants.LIST_COLUMNS
                    , cols.stream().map((col) -> col.getColName()).collect(Collectors.joining(String.valueOf(SerDeUtils.COMMA))));
            props.setProperty(serdeConstants.LIST_COLUMN_TYPES
                    , cols.stream().map((col) -> col.getType()).collect(Collectors.joining(String.valueOf(SerDeUtils.COMMA))));
            return props;
        }

        @Override
        public String getSerializationLib() {
            return this.serdeInfo.getSerializationLib();
        }
    }

    @Override
    public HiveTable getTable(String database, String tableName) {
        try {
            final Table table = storeClient.getTable(database, tableName);
            StorageDescriptor storageDesc = table.getSd();

            final List<HiveTabColType> cols = storageDesc.getCols()
                    .stream().map((col) -> new HiveTabColType(col.getName(), col.getType())).collect(Collectors.toUnmodifiableList());

            return new HiveTable(table.getTableName()) {

                @Override
                public List<HiveTabColType> getCols() {
                    return cols;
                }

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
                    return new HiveStoredAs(storageDesc);
                }

                @Override
                public List<String> listPaths(PartitionFilter filter) {
                    return filter.listStorePaths(new DefaultHiveTableContext(this, table, database, tableName, storeClient));
                }

                @Override
                public String getStorageLocation() {
                    try {
                        URI location = new URI(storageDesc.getLocation());
                        return location.getPath();
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
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