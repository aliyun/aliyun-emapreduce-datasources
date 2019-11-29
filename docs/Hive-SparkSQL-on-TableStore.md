# Hive/SparkSQL on TableStore

## Command-Line Interface

### Hive CLI

```
$ HADOOP_HOME=YourHadoopDir HADOOP_CLASSPATH=emr-tablestore-2.0.0-SNAPSHOT.jar:tablestore-4.1.0-jar-with-dependencies.jar:joda-time-2.9.4.jar bin/hive
```

### SparkSQL CLI

```
$ bin/spark-sql --master local --jars emr-tablestore-2.0.0-SNAPSHOT.jar,tablestore-4.1.0-jar-with-dependencies.jar
```


## Prepare `pet` table in TableStore

Let us prepare a `pet` table as an example (which is picked from [MySQL](http://dev.mysql.com/doc/refman/5.7/en/selecting-all.html))

| name     | owner  | species | sex  | birth      | death      |
|----------|--------|---------|------|------------|------------|
| Fluffy   | Harold | cat     | f    | 1993-02-04 |        |
| Claws    | Gwen   | cat     | m    | 1994-03-17 |        |
| Buffy    | Harold | dog     | f    | 1989-05-13 |        |
| Fang     | Benny  | dog     | m    | 1990-08-27 |        |
| Bowser   | Diane  | dog     | m    | 1979-08-31 | 1995-07-29 |
| Chirpy   | Gwen   | bird    | f    | 1998-09-11 |        |
| Whistler | Gwen   | bird    |  | 1997-12-09 |        |
| Slim     | Benny  | snake   | m    | 1996-04-29 |        |
| Puffball | Diane  | hamster | f    | 1999-03-30 |        |

First,  we have to prepare an empty `pet` table whose primary key is `name` of string type.

Second, we create an external table in Hive/SparkSQL.

```
CREATE EXTERNAL TABLE pet
  (name STRING, owner STRING, species STRING, sex STRING, birth STRING, death STRING)
  STORED BY 'com.aliyun.openservices.tablestore.hive.TableStoreStorageHandler'
  WITH SERDEPROPERTIES(
    "tablestore.columns.mapping"="name,owner,species,sex,birth,death")
  TBLPROPERTIES (
    "tablestore.endpoint"="YourEndpoint",
    "tablestore.access_key_id"="YourAccessKeyId",
    "tablestore.access_key_secret"="YourAccessKeySecret",
    "tablestore.table.name"="pet");
```

Third, we insert data into this external table.

```
INSERT INTO pet VALUES("Fluffy", "Harold", "cat", "f", "1993-02-04", null);
INSERT INTO pet VALUES("Claws", "Gwen", "cat", "m", "1994-03-17", null);
INSERT INTO pet VALUES("Buffy", "Harold", "dog", "f", "1989-05-13", null);
INSERT INTO pet VALUES("Fang", "Benny", "dog", "m", "1990-08-27", null);
INSERT INTO pet VALUES("Bowser", "Diane", "dog", "m", "1979-08-31", "1995-07-29");
INSERT INTO pet VALUES("Chirpy", "Gwen", "bird", "f", "1998-09-11", null);
INSERT INTO pet VALUES("Whistler", "Gwen", "bird", null, "1997-12-09", null);
INSERT INTO pet VALUES("Slim", "Benny", "snake", "m", "1996-04-29", null);
INSERT INTO pet VALUES("Puffball", "Diane", "hamster", "f", "1999-03-30", null);
```

Insertion into TableStore is updating related rows rather than replacing them.
So one can map some columns of a table in TableStore to one Hive/SparkSQL table and the others to another Hive/SparkSQL table.

TableStore is an internet-scale NoSQL database.
As most of large-scale NoSQL database, TableStore does not support transactions.
Because of lack of transactions, Hive `delete from` and `update from` are not applicable on TableStore tables.

### Query tables

Then we can query this `pet` table.

```
> SELECT * FROM pet;
Bowser  Diane   dog     m       1979-08-31      1995-07-29
Buffy   Harold  dog     f       1989-05-13      NULL
Chirpy  Gwen    bird    f       1998-09-11      NULL
Claws   Gwen    cat     m       1994-03-17      NULL
Fang    Benny   dog     m       1990-08-27      NULL
Fluffy  Harold  cat     f       1993-02-04      NULL
Puffball        Diane   hamster f       1999-03-30      NULL
Slim    Benny   snake   m       1996-04-29      NULL
Whistler        Gwen    bird    NULL    1997-12-09      NULL

> SELECT * FROM pet WHERE birth > "1995-01-01";
Chirpy  Gwen    bird    f       1998-09-11      NULL
Puffball        Diane   hamster f       1999-03-30      NULL
Slim    Benny   snake   m       1996-04-29      NULL
Whistler        Gwen    bird    NULL    1997-12-09      NULL
```

### Parameters

* `WITH SERDEPROPERTIES`
  + `tablestore.columns.mapping` (optional)

    By default, field names of external tables (in lower case by rules of Hive) are the same as column names (names of primary key columns or attributes) in TableStore.
    Sometimes, we need them to be different (for examples, dealing with case-sensitivity or charsets).
    We can then set `tablestore.columns.mapping`, which is a comma-separated string, each item is a column name, in the same order of fields.
    Please notice: don't leave blank before or after comma.

* `TBLPROPERTIES`
  + `tablestore.endpoint` (required)

    The [endpoint](https://help.aliyun.com/document_detail/27285.html?spm=5176.doc27281.6.109.8I57sZ).

  + `tablestore.instance` (optional)

    The [instance](https://help.aliyun.com/document_detail/27285.html?spm=5176.doc27281.6.109.p3M4Qa).
    If missing, it will be the part between `http://` (or `https://`) and the first dot.

  + `tablestore.table.name` (required)

    The table name in TableStore.

  + `tablestore.access_key_id`, `tablestore.access_key_secret` (required) and `tablestore.security_token` (optional)

    [credential](https://help.aliyun.com/document_detail/27296.html?spm=5176.doc27360.6.136.3FDlzl) and [Security Token](https://help.aliyun.com/document_detail/27360.html?spm=5176.doc27296.6.236.4AFNPp).

### Data type conversion

TableStore and Hive supports different sets of data types.
Here is the conversion rules (from TableStore (rows) to Hive (columns)):

| | TINYINT | SMALLINT | INT | BIGINT | FLOAT | DOUBLE | BOOLEAN | STRING | BINARY |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| INTEGER | Yes, loss of precision | Yes, loss of precision |　Yes, loss of precision | Yes | Yes, loss of precision | Yes, loss of precision | | | |
| DOUBLE | Yes, loss of precision | Yes, loss of precision | Yes, loss of precision | Yes, loss of precision | Yes, loss of precision | Yes | | | |
| BOOLEAN | | | | | | | Yes | | |
| STRING | | | | | | | | Yes | |
| BINARY | | | | | | | | | Yes |