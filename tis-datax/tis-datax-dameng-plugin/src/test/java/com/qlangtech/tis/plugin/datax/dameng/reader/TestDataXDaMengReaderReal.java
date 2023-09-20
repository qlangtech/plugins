package com.qlangtech.tis.plugin.datax.dameng.reader;

import com.qlangtech.tis.plugin.datax.dameng.ds.DaMengDataSourceFactory;
import com.qlangtech.tis.plugin.datax.dameng.ds.TestDaMengDataSourceFactory;
import com.qlangtech.tis.plugin.datax.dameng.writer.DataXDaMengWriter;
import org.junit.runners.Parameterized;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/15
 */

public class TestDataXDaMengReaderReal extends BasicRDBMSDataXReaderTest<DataXDaMengReader, DataXDaMengWriter, DaMengDataSourceFactory> {

    @Parameterized.Parameters(name = "{0}")
    public static Object[][] data() {
        return new Object[][]{ //
                {DataXDaMengReader.getDftTemplate(), DataXDaMengWriter.class}
        };
    }

    public TestDataXDaMengReaderReal(String dataXReaderConfig, Class<DataXDaMengWriter> clazz) {
        super(dataXReaderConfig, clazz);
    }

    @Override
    protected DataXDaMengReader createDataXReader(DaMengDataSourceFactory daMengDataSource) {
        final DataXDaMengReader dataxReader = new DataXDaMengReader() {
            @Override
            public Class<?> getOwnerClass() {
                return DataXDaMengReader.class;
            }

            @Override
            public DaMengDataSourceFactory getDataSourceFactory() {
                return daMengDataSource;
            }
        };
        return dataxReader;
    }

    @Override
    protected DaMengDataSourceFactory createDataSourceFactory() {
        DaMengDataSourceFactory dsFactory = TestDaMengDataSourceFactory.createDaMengDataSourceFactory();

        return dsFactory;
    }

    //   private SelectedTab createFullTypes() {

//        SelectedTab tab = new SelectedTab();
//        final String pkKey = "id";
//        tab.primaryKeys = Lists.newArrayList(pkKey);
//
//        List<CMeta> cols = Lists.newArrayList();
//
//
//        int pk = 2;
//        cols.add( CMeta.create( "id", ));
//        vals.put("tiny_c", RowValsExample.RowVal.$((byte) 255));
//        vals.put("tiny_un_c", RowValsExample.RowVal.$((byte) 127));
//        vals.put("small_c", RowValsExample.RowVal.$((short) 32767));
//        vals.put("small_un_c", RowValsExample.RowVal.$((short) 5534));
//        vals.put("medium_c", RowValsExample.RowVal.$(8388607));
//        vals.put("medium_un_c", RowValsExample.RowVal.$(16777215l));// MEDIUMINT UNSIGNED,
//        vals.put("int_c", RowValsExample.RowVal.$(2147483647));
//        vals.put("int_un_c", RowValsExample.RowVal.$(4294967295l)); //INTEGER UNSIGNED,
//        vals.put("int11_c", RowValsExample.RowVal.$(2147483647));
//        vals.put("big_c", RowValsExample.RowVal.$(9223372036854775807l));
//        vals.put("big_un_c", RowValsExample.RowVal.$(9223372036854775807l));
//        vals.put("varchar_c", RowValsExample.RowVal.$("Hello World"));
//        vals.put("char_c", RowValsExample.RowVal.$("abc"));
//        vals.put("real_c", RowValsExample.RowVal.$(123.102d));
//        vals.put("float_c", RowValsExample.RowVal.$(123.102f));
//        vals.put("double_c", RowValsExample.RowVal.$(404.4443d));
//        vals.put("decimal_c", RowValsExample.RowVal.decimal(1234567l, 4));
//        vals.put("numeric_c", RowValsExample.RowVal.decimal(3456, 0));
//        vals.put("big_decimal_c", RowValsExample.RowVal.decimal(345678921, 1));
//        vals.put("bit1_c", RowValsExample.RowVal.bit(true));
//        vals.put("tiny1_c", RowValsExample.RowVal.bit(true));
//        vals.put("boolean_c", RowValsExample.RowVal.bit(true));
//
//        vals.put("date_c", parseDate("2020-07-17"));
//
//        vals.put("time_c", RowValsExample.RowVal.time("18:00:22"));
//        vals.put("datetime3_c", parseTimestamp("2020-07-17 18:00:22"));
//        vals.put("datetime6_c", parseTimestamp("2020-07-17 18:00:22"));
//        vals.put("timestamp_c", parseTimestamp("2020-07-17 18:00:22"));
//        vals.put("file_uuid", RowValsExample.RowVal.stream(StringUtils.lowerCase("FA34E10293CB42848573A4E39937F479")
//                , (raw) -> {
//                    StringBuffer result = new StringBuffer();
//                    int val;
//                    for (int offset = 0; offset < 4; offset++) {
//                        result.append(Integer.toHexString(IntegerUtils.intFromByteArray(raw, offset * 4)));
//                    }
//                    return result.toString();
//                }).setSqlParamDecorator(() -> "UNHEX(?)"));
//        String colBitC = "bit_c";
//        vals.put(colBitC, RowValsExample.RowVal.stream("val", (raw) -> {
//            //TODO 暂时先让测试通过
//            return "val";
//        })); //b'0000010000000100000001000000010000000100000001000000010000000100'
//        vals.put("text_c", RowValsExample.RowVal.$("text"));
//        vals.put("tiny_blob_c", RowValsExample.RowVal.stream("blob_c"));
//        vals.put("blob_c", RowValsExample.RowVal.stream("blob_c"));
//        vals.put("medium_blob_c", RowValsExample.RowVal.stream("medium_blob_c"));
//        vals.put("long_blob_c", RowValsExample.RowVal.stream("long_blob_c_long_blob_c"));
//        vals.put("year_c", RowValsExample.RowVal.$(2021));
//        vals.put("enum_c", RowValsExample.RowVal.$("white"));//  default 'd',
//        vals.put("set_c", RowValsExample.RowVal.$("a,b"));
//        vals.put("json_c", RowValsExample.RowVal.$("{\"key1\":\"value1\"}"));
//                vals.put("point_c", RowValsExample.RowVal.$("ST_GeomFromText('POINT(1 1)')"));
//                vals.put("geometry_c", RowValsExample.RowVal.stream("ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))')"));
//                vals.put("linestring_c", RowValsExample.RowVal.$("ST_GeomFromText('LINESTRING(3 0, 3 3, 3 5)')"));
//                vals.put("polygon_c", RowValsExample.RowVal.$("ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))')"));
//                vals.put("multipoint_c", RowValsExample.RowVal.$("ST_GeomFromText('MULTIPOINT((1 1),(2 2))')"));
//                vals.put("multiline_c", RowValsExample.RowVal.$("ST_GeomFromText('MultiLineString((1 1,2 2,3 3),(4 4,5 5))')"));
//                vals.put("multipolygon_c", RowValsExample.RowVal.$("ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))')"));
//                vals.put("geometrycollection_c", RowValsExample.RowVal.$("ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))')"));
    // }

}
