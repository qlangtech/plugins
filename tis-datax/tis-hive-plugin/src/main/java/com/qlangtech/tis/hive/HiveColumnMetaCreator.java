package com.qlangtech.tis.hive;

import com.qlangtech.tis.plugin.ds.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/8/28
 */
public class HiveColumnMetaCreator extends DataSourceFactory.CreateColumnMeta {
    public HiveColumnMetaCreator(Set<String> pkCols, ResultSet columns1) {
        super(pkCols, columns1);
    }

    protected DataType createColDataType(String colName, String typeName, int dbColType, int colSize, int decimalDigits) throws SQLException {
        DataTypeMeta dataTypeMeta = DataTypeMeta.getDataTypeMeta(JDBCTypes.parse(dbColType));
        ColSizeRange colsSizeRange = null;
        if (dataTypeMeta == null) {
            return super.createColDataType(colName, typeName, dbColType, colSize, decimalDigits);
        }
        if (dataTypeMeta.isContainColSize()) {
            colsSizeRange = dataTypeMeta.getColsSizeRange();
            return super.createColDataType(colName, typeName, dbColType, colsSizeRange.rectify(colSize), decimalDigits);
        }

        return super.createColDataType(colName, typeName, dbColType, colSize, decimalDigits);
    }
}
