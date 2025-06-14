package com.qlangtech.tis.hive;

import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.HiveTable.HiveTabColType;
import com.qlangtech.tis.config.hive.meta.HiveTable.StoredAs;
import com.qlangtech.tis.config.hive.meta.IHiveMetaStore;
import com.qlangtech.tis.config.hive.meta.PartitionFilter;
import com.qlangtech.tis.hive.shim.IHiveSerDe;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
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
    private final HiveConf hiveCfg;
    private static final Logger logger = LoggerFactory.getLogger(DefaultHiveMetaStore.class);

    public DefaultHiveMetaStore(HiveConf hiveCfg, IMetaStoreClient storeClient, String metaStoreUrls) {
        this.storeClient = storeClient;
        this.metaStoreUrls = metaStoreUrls;
        this.hiveCfg = hiveCfg;
    }

    @Override
    public String getServerVersion() {
//        try {
//           // return this.storeClient.getServerVersion();
//        } catch (TException e) {
//            throw new RuntimeException(e);
//        }a
        return null;
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
            return tables.stream().map((tab) -> new HiveTable(tab, Collections.emptyMap()) {
//                @Override
//                public StoredAs getStoredAs() {
//                    throw new UnsupportedOperationException();
//                }


                @Override
                public <CONFIG> StoredAs getStoredAs(CONFIG conf, ClassLoader classLoader) {
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

    @Override
    public HiveConf getHiveCfg() {
        return Objects.requireNonNull(this.hiveCfg, "hiveCfg can not be null");
    }

    @Override
    public IMetaStoreClient unwrapClient() {
        return this.storeClient;
    }

    public static class HiveStoredAs extends StoredAs {
        private final SerDeInfo serdeInfo;
        private final InputFormat inputFormat;
        private final Class inputFormatClass;
        private final HiveOutputFormat outputFormat;
        //  private final org.apache.hadoop.hive.serde2.SerDe serde;

        private final IHiveSerDe serDe;

        private final JobConf jobConf;
        private final Properties tabStoreProps;

        public HiveStoredAs(HiveTable table, StorageDescriptor storageDesc
                , org.apache.hadoop.conf.Configuration conf, ClassLoader classLoader) {
            super();
            this.serdeInfo = Objects.requireNonNull(storageDesc.getSerdeInfo(), "serdeInfo");
            this.tabStoreProps = this.getSerdeProperties(table);

            try {
                this.inputFormatClass = Class.forName(storageDesc.getInputFormat()
                        , false, Objects.requireNonNull(classLoader, "classLoader can not be null"));
                Class outputFormatClass = Class.forName(storageDesc.getOutputFormat(), false, classLoader);
                // forExample : LazySimpleSerDe, ParquetHiveSerDe
                serDe = IHiveSerDe.create(classLoader);


//                org.apache.hadoop.hive.serde2.SerDe serde = (org.apache.hadoop.hive.serde2.SerDe) Class.forName(
//                        serdeInfo.getSerializationLib()
//                        , false, classLoader).getDeclaredConstructor().newInstance();

                this.jobConf = new JobConf(conf);
                // serde.initialize(serdeInfo.getSerializationLib(),jobConf, tabStoreProps);

                serDe.initialize(serdeInfo.getSerializationLib(), jobConf, tabStoreProps, classLoader);


                this.inputFormat = (InputFormat) inputFormatClass.getConstructor().newInstance();
                this.outputFormat = (HiveOutputFormat) outputFormatClass.getConstructor().newInstance();
                //   this.serde = serde;

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        public Properties getTabStoreProps() {
            return this.tabStoreProps;
        }

        public InputFormat getInputFormat() {
            return inputFormat;
        }

        public HiveOutputFormat getOutputFormat() {
            return outputFormat;
        }

        public IHiveSerDe getSerde() {
            return serDe;
        }

        public JobConf getJobConf() {
            return jobConf;
        }

        public Class getInputFormatClass() {
            return this.inputFormatClass;
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
                    , cols.stream().map((col) -> {
                        return convertType(col.getType());
                    }).collect(Collectors.joining(String.valueOf(SerDeUtils.COMMA))));
            return props;
        }

        protected String convertType(String type) {
            return type;
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
            final Map<String, String> tabParameters = table.getParameters();
            StorageDescriptor storageDesc = table.getSd();

            final List<HiveTabColType> cols = storageDesc.getCols()
                    .stream().map((col) -> new HiveTabColType(col.getName(), col.getType())).collect(Collectors.toUnmodifiableList());

            return new HiveTable(table.getTableName(), tabParameters) {

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
                public <CONFIG> StoredAs getStoredAs(CONFIG conf, ClassLoader classLoader) {
                    return new HiveStoredAs(this, storageDesc, (org.apache.hadoop.conf.Configuration) conf, classLoader);
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