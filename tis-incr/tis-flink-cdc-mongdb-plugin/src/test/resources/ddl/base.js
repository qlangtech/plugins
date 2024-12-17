

db.getCollection('base').insertMany([
    { "_id": ObjectId("5d505646cf6d4fe581014ab2")
    ,"base_id": NumberInt("101")
    , "start_time": ISODate("2019-08-11T17:54:14.692Z")
    ,  "update_date": ISODate("2019-08-11")
    , "update_time": Timestamp(1565545664,1)
    ,"price": NumberDecimal("10.99")
    ,"json_content":{"a":"hello", "b": NumberLong("50")}
    ,"col_blob":BinData(0,"AQID")
    ,"col_text":"hello" }
]);

// 测试用
//db.getCollection('full_types').insertOne({
//    "_id": ObjectId("4d505646cf6d4fe581014abf"),
//    "stringField": "hello baisui",
//    "uuidField": UUID("cbd1e27e-2829-4b47-8e21-dfef93da44e1"),
//    "md5Field": MD5("2078693f4c61ce3073b01be69ab76428"),
//    "timeField": ISODate("2019-08-11T17:54:14.692Z"),
//    "dateField": ISODate("2019-08-11T17:54:14.692Z"),
//    "dateBefore1970": ISODate("1960-08-11T17:54:14.692Z"),
//    "dateToTimestampField": ISODate("2019-08-11T17:54:14.692Z"),
//    "dateToLocalTimestampField": ISODate("2019-08-11T17:54:14.692Z"),
//    "timestampField": Timestamp(1565545664,1),
//    "timestampToLocalTimestampField": Timestamp(1565545664,1),
//    "booleanField": true,
//    "decimal128Field": NumberDecimal("10.99"),
//    "doubleField": 10.5,
//    "int32field": NumberInt("10"),
//    "int64Field": NumberLong("50"),
//    "documentField": {"a":"hello", "b": NumberLong("50")},
//    "mapField": {
//        "inner_map": {
//            "key": NumberLong("234")
//        }
//    },
//    "arrayField": ["hello","world"],
//    "doubleArrayField": [1.0, 1.1, null],
//    "documentArrayField": [{"a":"hello0", "b": NumberLong("51")}, {"a":"hello1", "b": NumberLong("53")}],
//    "minKeyField": MinKey(),
//    "maxKeyField": MaxKey(),
//    "regexField": /^H/i,
//    "undefinedField": undefined,
//    "nullField": null,
//    "binaryField": BinData(0,"AQID"),
//    "javascriptField": (function() { x++; }),
//    "dbReferenceField": DBRef("ref_doc", ObjectId("5d505646cf6d4fe581014ab3"))
//});
//
//db.getCollection('full_types').updateOne({
//    "_id": ObjectId("4d505646cf6d4fe581014abf")}
//,  { $set: { stringField: "hello baisuii", uuidField: UUID("11111111-2829-4b47-8e21-dfef93da44e1"), booleanField: false } }
//    );
//
//
//db.getCollection('full_types').replaceOne({
//    "_id": ObjectId("4d505646cf6d4fe581014abf")}
//,   { stringField: "hello baisuiiiiiiii", uuidField: UUID("11111112-2829-4b47-8e21-dfef93da44e1") , booleanField: true  }
//    );
//
//
//db.getCollection('full_types').deleteOne({
//    "_id": ObjectId("4d505646cf6d4fe581014abf")}
//);