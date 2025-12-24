/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.plugins.incr.flink.cdc.mongdb;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.DataXMongodbReader;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

/**
 * https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/mongodb-cdc/
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-02 11:36
 **/
@Public
public class FlinkCDCMongoDBSourceFactory extends MQListenerFactory {

    @FormField(ordinal = 1, validate = {Validator.require})
    public MongoCDCStartupOptions startupOption;

    /**
     * https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/mongodb-cdc/#full-changeloga-namefull-changelog-id003-a
     */
    @FormField(ordinal = 9, validate = {Validator.require})
    public UpdateRecordComplete updateRecordComplete;


    @FormField(ordinal = 10, advance = true, type = FormFieldType.INPUTTEXT)
    public String connectionOptions;

//    @FormField(ordinal = 11, advance = true, type = FormFieldType.TEXTAREA)
//    public String copyExistingPipeline;

    //private transient IConsumerHandle consumerHandle;

    @Override
    public IFlinkColCreator<FlinkCol> createFlinkColCreator(DataSourceMeta sourceMeta) {
        return (meta, colIndex) -> {
            return meta.getType().accept(new MongoDBCDCTypeVisitor(meta, colIndex));
        };
    }

    @Override
    public IMQListener create() {
        FlinkCDCMongoDBSourceFunction sourceFunction = new FlinkCDCMongoDBSourceFunction(this);
        return sourceFunction;
    }


    /**
     * 还没有调试好暂时不支持
     */
    @TISExtension()
    public static class DefaultDescriptor extends BaseDescriptor {
        @Override
        public String getDisplayName() {
            return "Flink-CDC-MongoDB";
        }

        @Override
        protected boolean validateMQListenerForm(IControlMsgHandler msgHandler, Context context, MQListenerFactory sourceFactory) {
            DataXName pipe = msgHandler.getCollectionName();
            DataXMongodbReader mongoReader = (DataXMongodbReader) DataxReader.load(null, pipe.getPipelineName());
            MangoDBDataSourceFactory dsFactory = mongoReader.getDataSourceFactory();

            try {
                // Validate MongoDB CDC prerequisites
                return dsFactory.vistMongoClient((mongoClient) -> {
                    try {
                        // 1. Check if MongoDB is running in Replica Set or Sharded Cluster mode
                        validateReplicaSetMode(mongoClient, msgHandler, context);

                        // 2. Check user permissions for CDC operations
                        validateCDCPermissions(mongoClient, dsFactory, msgHandler, context);

                        return true;
                    } catch (Exception e) {
                        msgHandler.addErrorMessage(context, "MongoDB CDC validation failed: " + e.getMessage());
                        return false;
                    }
                });
            } catch (Exception e) {
                msgHandler.addErrorMessage(context, "Failed to connect to MongoDB: " + e.getMessage());
                return false;
            }
        }

        /**
         * Validate that MongoDB is running in Replica Set or Sharded Cluster mode
         * MongoDB CDC requires replica set mode to capture change events from oplog
         */
        private void validateReplicaSetMode(com.mongodb.client.MongoClient mongoClient, IControlMsgHandler msgHandler, Context context) {
            try {
                com.mongodb.client.MongoDatabase adminDb = mongoClient.getDatabase("admin");
                org.bson.Document isMasterResult = adminDb.runCommand(new org.bson.Document("isMaster", 1));

                // Check if it's a replica set member
                boolean isReplicaSet = isMasterResult.containsKey("setName");
                // Check if it's a sharded cluster (mongos)
                boolean isSharded = "isdbgrid".equals(isMasterResult.getString("msg"));

                if (!isReplicaSet && !isSharded) {
                    throw new IllegalStateException(
                            "MongoDB CDC requires Replica Set or Sharded Cluster mode. " +
                            "Current MongoDB instance is running in standalone mode. " +
                            "Please configure MongoDB as a replica set to enable CDC functionality.");

                }
            } catch (com.mongodb.MongoCommandException e) {
                throw new RuntimeException("Failed to execute isMaster command, please check MongoDB connection and permissions", e);
            }
        }

        /**
         * Validate user has necessary permissions for CDC operations:
         * 1. Read access to local database (for oplog)
         * 2. Read access to target database
         * 3. Permission to execute changeStream operations
         */
        private void validateCDCPermissions(com.mongodb.client.MongoClient mongoClient,
                                           MangoDBDataSourceFactory dsFactory,
                                           IControlMsgHandler msgHandler,
                                           Context context) {
            try {
                // Check permission to access local database (required for oplog access)
                com.mongodb.client.MongoDatabase localDb = mongoClient.getDatabase("local");
                try {
                    // Try to list collections in local database
                    localDb.listCollectionNames().first();
                } catch (com.mongodb.MongoCommandException e) {
                    throw new IllegalStateException(
                            "User does not have permission to read 'local' database. " +
                            "MongoDB CDC requires read access to local database to capture change events. " +
                            "Please grant the following role to user '" + dsFactory.getUserName() + "': " +
                            "{ role: 'read', db: 'local' } or use the 'changeStream' role.");
                }

                // Check permission to access target database
                com.mongodb.client.MongoDatabase targetDb = mongoClient.getDatabase(dsFactory.getDbName());
                try {
                    // Try to list collections in target database
                    targetDb.listCollectionNames().first();
                } catch (com.mongodb.MongoCommandException e) {
                    throw new IllegalStateException(
                            "User does not have permission to read database '" + dsFactory.getDbName() + "'. " +
                            "Please grant read permission to user '" + dsFactory.getUserName() + "' on database '" + dsFactory.getDbName() + "'.");
                }

                // Check if user can execute changeStream (try to create a dummy change stream)
                try {
                    // Attempt to create a change stream on the target database
                    // This validates that the user has the necessary permissions
                    com.mongodb.client.ChangeStreamIterable<org.bson.Document> changeStream =
                        targetDb.watch();
                    // Close the cursor immediately, we just need to verify permissions
                    changeStream.cursor().close();
                } catch (com.mongodb.MongoCommandException e) {
                    // Check if it's a permission error
                    if (e.getErrorCode() == 13) { // Unauthorized error code
                        throw new IllegalStateException(
                                "User does not have permission to execute changeStream operations. " +
                                "Please grant the 'changeStream' privilege or 'read' role on database '" + dsFactory.getDbName() + "' " +
                                "to user '" + dsFactory.getUserName() + "'.");
                    }
                    // Other errors might be acceptable (e.g., if the collection doesn't exist yet)
                }

            } catch (IllegalStateException e) {
                // Re-throw IllegalStateException with our custom error messages
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to validate CDC permissions: " + e.getMessage(), e);
            }
        }

        @Override
        public PluginVender getVender() {
            return PluginVender.FLINK_CDC;
        }

        @Override
        public IEndTypeGetter.EndType getEndType() {
            return IEndTypeGetter.EndType.MongoDB;
        }
    }
}
