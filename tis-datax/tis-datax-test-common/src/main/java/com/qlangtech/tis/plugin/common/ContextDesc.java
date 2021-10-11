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

import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-23 14:33
 **/
public class ContextDesc {

    public static void descBuild(Class<?> clazz, boolean reader) {

        Field[] declaredFields = clazz.getDeclaredFields();
        FormField ff = null;
        // Validator[] validate = null;
        StringBuffer buffer = new StringBuffer();
        StringBuffer template = new StringBuffer();
        StringBuffer optionProp = new StringBuffer();
        boolean propRequire;
        for (Field f : declaredFields) {

            ff = f.getAnnotation(FormField.class);
            if (ff == null) {
                continue;
            }

            propRequire = propRequire(ff);
            //   validate = ff.validate();
            if (!propRequire) {
                buffer.append("public boolean isContain").append(StringUtils.capitalize(f.getName()))
                        .append("(){ \n");
                if (f.getType() == String.class) {
                    buffer.append("\t return StringUtils.isNotBlank(this.").append(getRefName(reader)).append(".").append(f.getName()).append(");\n");
                } else {
                    buffer.append("\t return this.").append(getRefName(reader)).append(".").append(f.getName()).append("!= null;\n");
                }
                buffer.append("}\n");

                optionProp.append(f.getName()).append("\n");
            }


            buffer.append(" public ").append(f.getType().getSimpleName()).append(" get").append(StringUtils.capitalize(f.getName())).append("() { \n");
            buffer.append("    return this.").append(getRefName(reader)).append(".").append(f.getName()).append(";\n");
            buffer.append(" }\n");


            if (propRequire) {
                appendPropTpl(reader, template, f);
            } else {
                template.append("#if($").append(getRefName(reader)).append(".contain").append(StringUtils.capitalize(f.getName())).append(")\n");
                template.append("\t\t");
                appendPropTpl(reader, template, f);
                template.append("#end\n");
            }


        }


//        PropertyUtilsBean propertyUtils = BeanUtilsBean.getInstance().getPropertyUtils();
//        PropertyDescriptor[] propertyDescriptors = propertyUtils.getPropertyDescriptors(clazz);
//        for (PropertyDescriptor desc : propertyDescriptors) {
//            System.out.println(desc.getName() + ":" + desc.getPropertyType());
//            buffer.append("public boolean isContain").append(StringUtils.capitalize(desc.getName()))
//                    .append("(){ \n");
//            if (desc.getPropertyType() == String.class) {
//                buffer.append("\t return StringUtils.isNotBlank(this.writer.").append(desc.getName()).append(");\n");
//            } else {
//                buffer.append("\t return this.writer.").append(desc.getName()).append("!= null;\n");
//            }
//            buffer.append("}\n");
//        }

        System.out.println(buffer.toString());
        System.out.println("===========================================");
        System.out.println(template.toString());
        System.out.println("option props===========================================");
        System.out.println(optionProp.toString());
    }

    protected static String getRefName(boolean reader) {
        return reader ? "reader" : "writer";
    }

    protected static void appendPropTpl(boolean reader, StringBuffer template, Field f) {

        boolean strType = (f.getType() == String.class);

        template.append(" ,\"").append(f.getName()).append("\": ").append(strType ? "\"" : StringUtils.EMPTY)
                .append("${").append(getRefName(reader)).append(".").append(f.getName()).append("}").append(strType ? "\"" : StringUtils.EMPTY).append("\n");
    }

    private static boolean propRequire(FormField ff) {
        Validator[] validate = ff.validate();
        for (Validator v : validate) {
            if (v == Validator.require) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) {

    }
}
