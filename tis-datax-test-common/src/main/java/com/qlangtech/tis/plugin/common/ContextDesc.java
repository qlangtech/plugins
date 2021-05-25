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

package com.qlangtech.tis.plugin.common;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.commons.lang.StringUtils;

import java.beans.PropertyDescriptor;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-23 14:33
 **/
public class ContextDesc {

    public static void descBuild(Class<?> clazz) {
        StringBuffer buffer = new StringBuffer();
        PropertyUtilsBean propertyUtils = BeanUtilsBean.getInstance().getPropertyUtils();
        PropertyDescriptor[] propertyDescriptors = propertyUtils.getPropertyDescriptors(clazz);
        for (PropertyDescriptor desc : propertyDescriptors) {
            System.out.println(desc.getName() + ":" + desc.getPropertyType());
            buffer.append("public boolean isContain").append(StringUtils.capitalize(desc.getName()))
                    .append("(){ \n");
            if (desc.getPropertyType() == String.class) {
                buffer.append("\t return StringUtils.isNotBlank(this.writer.").append(desc.getName()).append(");\n");
            } else {
                buffer.append("\t return this.writer.").append(desc.getName()).append("!= null;\n");
            }
            buffer.append("}\n");
        }

        System.out.println(buffer.toString());
    }

    public static void main(String[] args) {

    }
}
