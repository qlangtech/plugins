 mvn deploy:deploy-file \
 -DgroupId=com.qlangtech.tis.sqlserver \
 -DartifactId=mssql-jdbc \
 -Dversion=4.2.jre8 \
 -Dpackaging=jar \
 -Dfile=/Users/mozhenghua/Downloads/sqljdbc_4.2/chs/jre8/sqljdbc42.jar \
 -Durl=http://localhost:8080/release
