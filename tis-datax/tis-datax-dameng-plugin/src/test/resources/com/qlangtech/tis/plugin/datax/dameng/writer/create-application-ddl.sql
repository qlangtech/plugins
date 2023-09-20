CREATE TABLE "application_alias"
(
    "user_id"     BIGINT,
    "user_name"   VARCHAR2(256 CHAR),
    "id3"         BIGINT,
    "col4"        VARCHAR2(256 CHAR),
    "col5"        VARCHAR2(256 CHAR),
    "col6"        VARCHAR2(256 CHAR)
 , CONSTRAINT application_alias_pk PRIMARY KEY ("user_id")
)