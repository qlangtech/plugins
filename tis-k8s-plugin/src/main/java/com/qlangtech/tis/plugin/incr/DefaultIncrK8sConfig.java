/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.qlangtech.tis.plugin.incr;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.IRCController;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.PluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.k8s.K8sImage;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @create: 2020-04-12 11:06
 * @date 2020/04/13
 */
public class DefaultIncrK8sConfig extends IncrStreamFactory {

    public static final String KEY_FIELD_NAME = "k8sImage";

    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String k8sImage;

    private IRCController incrSync;

    @Override
    public IRCController getIncrSync() {
        if (incrSync != null) {
            return this.incrSync;
        }
        this.incrSync = new K8sIncrSync(TIS.getPluginStore(K8sImage.class).find(k8sImage));
        return this.incrSync;
    }

    @TISExtension()
    public static class DescriptorImpl extends Descriptor<IncrStreamFactory> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_FIELD_NAME, () -> {
                PluginStore<K8sImage> images = TIS.getPluginStore(K8sImage.class);
                return images.getPlugins();
            });
        }

        @Override
        public String getDisplayName() {
            return "k8s-incr";
        }
    }
}
