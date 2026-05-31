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
package com.qlangtech.tis.plugin.ontology.impl.objtype;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.qlangtech.tis.extension.MultiStepsSupportHostDescriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.OptionWithEndType;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologySharedProperty;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
import com.qlangtech.tis.plugin.ontology.TargetProperty;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.sync.OntologyNeo4jSyncService;
import com.qlangtech.tis.plugin.ontology.sync.OntologySyncQueue;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 * @see ObjectTypeProfile
 * @see ObjectTypeProperties
 * @see ObjectTypePropertiesRelevant
 */
public class DefaultOntologyObjectType extends OntologyObjectType implements IPluginStore.AfterPluginSaved {

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        String domain = OntologyPluginMeta.createPluginMeta(pluginContext.getContext()).getDomain();
        final DefaultOntologyObjectType self = this;
        OntologySyncQueue.enqueue(() -> OntologyNeo4jSyncService.getInstance().syncObjectType(domain, self));
    }

    @Override
    public String getName() {
        return this.getProfile().name;
    }

    private ObjectTypeProperties getPropsStep() {
        return (ObjectTypeProperties) getMultiStepsSavedItems()[1];
    }

    private ObjectTypePropertiesRelevant getPropertiesRelevantStep() {
        return (ObjectTypePropertiesRelevant) getMultiStepsSavedItems()[2];
    }

    public Optional<OntologyProperty> getPk() {
        return this.getPropsStep().getPk();
    }

    /**
     * 是否禁用了主键
     *
     * @return
     */
    @Override
    public boolean hasDisablePK() {
        return getPropertiesRelevantStep().pkSetter.hasDisablePK();
    }

    public final List<OntologyProperty> getCols() {
        return this.getPropsStep().getCols();
    }

    @JSONField(serialize = false)
    public final List<OptionWithEndType> getColOpts() {
        return this.getPropsStep().getColOpts();
    }

    @Override
    public ObjectTypeBinding.ObjectTypeBindingInfo getObjectTypeBindingInfo() {
        return this.getProfile().binding.getObjectTypeBindingInfo();
    }

    @Override
    public ObjectTypeBinding getDataSourceBinding() {
        return this.getProfile().binding;
    }

    @Override
    public void setNewDataSourceBinding(DataSourceFactory tagetDS) {
        DataSourceBinding newBinding = new DataSourceBinding();
        newBinding.dbName = Objects.requireNonNull(tagetDS).name;
        ObjectTypeProfile profile = this.getProfile();
        if (profile.binding instanceof DataSourceBinding old) {
            newBinding.physicalTableName = old.physicalTableName;
        }
        profile.binding = newBinding;
    }

    private ObjectTypeProfile getProfile() {
        return (ObjectTypeProfile) getMultiStepsSavedItems()[0];
    }

    @Override
    public void setSharedProperty(TargetProperty targetProperty, OntologySharedProperty sharedProp) {
        if (targetProperty == null) {
            throw new IllegalArgumentException("param targetProperty can not be null");
        }
        if (sharedProp == null) {
            throw new IllegalArgumentException("param sharedProp can not be null");
        }
        ObjectTypeProperties propsStep = getPropsStep();
        for (OntologyProperty property : propsStep.getCols()) {
            if (StringUtils.equals(targetProperty.property(), property.getName())) {
                property.reference2SharedProp(sharedProp);
                return;
            }
        }
        throw new IllegalStateException("can not find property:" + targetProperty + " in objectType:" + this.getName());
    }

    @Override
    public void setValeType(TargetProperty targetProperty, OntologyValueType valueType) {
        if (targetProperty == null) {
            throw new IllegalArgumentException("param targetProperty can not be null");
        }
        if (valueType == null) {
            throw new IllegalArgumentException("param valueType can not be null");
        }
        ObjectTypeProperties propsStep = getPropsStep();
        for (OntologyProperty property : propsStep.getCols()) {
            if (StringUtils.equals(targetProperty.property(), property.getName())) {
                property.reference2ValueType(valueType);
                return;
            }
        }
        throw new IllegalStateException("can not find property:" + targetProperty + " in objectType:" + this.getName());
    }

    @TISExtension
    public static class DefaultDesc extends BasicDesc implements MultiStepsSupportHostDescriptor<OntologyObjectType> {
        public DefaultDesc() {
            super();
        }

        @Override
        public EndType getEndType() {
            return EndType.OntologyObjectType;
        }

        @Override
        public OntologyEnum getOntologyType() {
            return OntologyEnum.ObjectType;
        }

        @Override
        public String getDisplayName() {
            return "Object Type";
        }

        @Override
        public Class<OntologyObjectType> getHostClass() {
            return OntologyObjectType.class;
        }

        @Override
        public List<OneStepOfMultiSteps.BasicDesc> getStepDescriptionList() {
            return List.of(new ObjectTypeProfile.DftDesc(), new ObjectTypeProperties.DftDesc(),
                    new ObjectTypePropertiesRelevant.DftDesc());
        }

        @Override
        public void appendExternalProps(JSONObject multiStepsCfg) {
        }

        @Override
        public String shortComment() {
            return "定义本体对象类型，对应数据源中的一张表";
        }
    }
}