/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;

import java.util.Iterator;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-25 11:09
 **/
public class DataXFtpReaderPlugin extends DataxReader {
    @Override
    public boolean hasMulitTable() {
        return false;
    }

    @Override
    public List<ISelectedTab> getSelectedTabs() {
        return null;
    }

    @Override
    public boolean hasExplicitTable() {
        return false;
    }

    @Override
    public Iterator<IDataxReaderContext> getSubTasks() {
        return null;
    }

    @Override
    public String getTemplate() {
        return null;
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<DataxReader> {
    }
}
