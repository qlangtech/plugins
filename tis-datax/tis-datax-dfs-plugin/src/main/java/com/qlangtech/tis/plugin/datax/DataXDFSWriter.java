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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.plugin.unstructuredstorage.Compress;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.datax.IDataXBatchPost;
import com.qlangtech.tis.datax.IDataXGenerateCfgs;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPostTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPreviousTrigger;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.format.BasicPainFormat.BasicPainFormatDescriptor;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import com.qlangtech.tis.plugin.datax.meta.MetaDataWriter;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.writer.ftpwriter.FtpWriter
 **/
@Public
public class DataXDFSWriter extends DataxWriter implements IDataXBatchPost {

    @FormField(ordinal = 1, validate = {Validator.require})
    public TDFSLinker dfsLinker;


    /**
     * 写入数据过程中会在ftp目录中写一份source的元数据
     */
    @FormField(ordinal = 7, validate = {Validator.require})
    public MetaDataWriter writeMetaData;

    @FormField(ordinal = 8, type = FormFieldType.ENUM, validate = {Validator.require})
    public String writeMode;

    @FormField(ordinal = 14, validate = {Validator.require})
    public FileFormat fileFormat;

    public static List<? extends Descriptor> supportedWriterFormat(List<? extends Descriptor> descs) {
        return BasicPainFormatDescriptor.supportedFormat(false, descs);
    }

    @Override
    public ExecutePhaseRange getPhaseRange() {
        return new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.FullDump);
    }

    @Override
    public EntityName parseEntity(ISelectedTab tab) {
        return Objects.requireNonNull(tab, "tab can not be null").getEntityName();
        //return EntityName.parse(tab.getName());
    }

    @Override
    public void startScanDependency() {
        try (ITDFSSession itdfsSession = dfsLinker.createTdfsSession()) {

        } catch (Exception e) {

        }
    }

    @Override
    public IRemoteTaskPreviousTrigger createPreExecuteTask(IExecChainContext execContext, EntityName entity, ISelectedTab tab) {
        return writeMetaData.createMetaDataWriteTask(this.dfsLinker, execContext, entity, tab);
    }

    @Override
    public IRemoteTaskPostTrigger createPostTask(IExecChainContext execContext, EntityName entity, ISelectedTab tab, IDataXGenerateCfgs cfgFileNames) {
        return null;
    }


    public static List<Option> supportCompress() {
        return Arrays.stream(Compress.values()).filter((c) -> c.supportWriter())
                .map((c) -> new Option(c.name(), c.token)).collect(Collectors.toList());
    }


    @FormField(ordinal = 17, type = FormFieldType.TEXTAREA, advance = false, validate = {Validator.require})
    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXDFSWriter.class, "DataXDFSWriter-tpl.json");
    }


    @Override
    public String getTemplate() {
        return this.template;
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        DataXDFSWriterContext writerContext = new DataXDFSWriterContext(this, tableMap.get(), transformerRules);
        return writerContext;
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxWriterDescriptor {
        public DefaultDescriptor() {
            super();
            // registerSelectOptions(KEY_FTP_SERVER_LINK, () -> ParamsConfig.getItems(FTPServer.FTP_SERVER));
        }

        @Override
        public boolean isSupportIncr() {
            return false;
        }

        @Override
        public EndType getEndType() {
            return EndType.TDFS;
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return getEndType().name();
        }

    }
}
