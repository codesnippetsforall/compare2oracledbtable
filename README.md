# compare2oracledbtable
Java code to compare 2 Oracle Databases and compare rows for a table(s)

Download the ojdbc8.jar
https://www.oracle.com/in/database/technologies/appdev/jdbc-downloads.html

Create a Java Project and add the ojdb8.jar as External Jar

**Source Code and its Purpose**

1. ConnectOracleAndFetchRecordCount
   
   Code to connect the Oracle database for a particular table and get the record count then print it
   
2. Compare2DatabaseWith1Table
   
   Code to connect 2 Oracle databases for a particular table and verify the difference.
   
3. Compare2DatabaseWith1TableWithPropertyLogs
   
   Code to connect 2 Oracle databases for a particular table using Property file reader.  Config data present in separate file i.e, DatabaseConfig.properties.  Code iterate all the rows in the source table and verify with target table.  In case of any difference then code get the primary key of the table and print the primary key value in the log file i.e, differences.log

4. Compare2DatabaseWith1TableWithPropertyLogsV1
   
* Oracle DB Source and Target Database Table comparison
* Get and Display Record count from Source and Target Table
* Source & Target Record count validation
* PrimaryKey field validation from Config file
* Display Message when Source & Target record not matched
and its Record Counter
* New Config file with additional line i.e., DatabaseConfigV1.properties
