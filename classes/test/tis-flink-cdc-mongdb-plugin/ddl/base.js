

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

