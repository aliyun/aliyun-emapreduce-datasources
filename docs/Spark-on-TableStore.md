# Spark on TableStore

## TableStore as Data Source

### Prepare a table

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

where `name` is the only primary key.
As TableStore is schema-free, we do not need to (and should not) write blank cells to the `pet` table.

### Count rows

Here is the JAVA program:

```java
private static RangeRowQueryCriteria fetchCriteria() {
    RangeRowQueryCriteria res = new RangeRowQueryCriteria("pet");
    res.setMaxVersions(1);
    List<PrimaryKeyColumn> lower = new ArrayList<PrimaryKeyColumn>();
    List<PrimaryKeyColumn> upper = new ArrayList<PrimaryKeyColumn>();
    lower.add(new PrimaryKeyColumn("name", PrimaryKeyValue.INF_MIN));
    upper.add(new PrimaryKeyColumn("name", PrimaryKeyValue.INF_MAX));
    res.setInclusiveStartPrimaryKey(new PrimaryKey(lower));
    res.setExclusiveEndPrimaryKey(new PrimaryKey(upper));
    return res;
}

public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("RowCounter");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    Configuration hadoopConf = new Configuration();
    JavaSparkContext sc = null;
    try {
        sc = new JavaSparkContext(sparkConf);
        Configuration hadoopConf = new Configuration();
        TableStore.setCredential(
                hadoopConf,
                new Credential(accessKeyId, accessKeySecret, securityToken));
        Endpoint ep = new Endpoint(endpoint, instance);
        TableStore.setEndpoint(hadoopConf, ep);
        TableStoreInputFormat.addCriteria(hadoopConf, fetchCriteria());
F
        JavaPairRDD<PrimaryKeyWritable, RowWritable> rdd = sc.newAPIHadoopRDD(
                hadoopConf, TableStoreInputFormat.class,
                PrimaryKeyWritable.class, RowWritable.class);
        System.out.println(
            new Formatter().format("TOTAL: %d", rdd.count()).toString());
    } finally {
        if (sc != null) {
            sc.close();
        }
    }
}
```

If you prefer to scala, please replace `JavaSparkContext` to `SparkContext` and `JavaPairRDD` to `PairRDD`.

Let it run.

```
$ bin/spark-submit --master local --jars emr-tablestore-2.0.0-SNAPSHOT.jar,tablestore-4.1.0-jar-with-dependencies.jar YourRowCounter.jar
TOTAL: 9
```

FYI,
* for more details about `TableStoreInputFormat`, please refer to (HadoopMR-on-TableStore.md).
* in emr-examples_2.11-2.0.0-SNAPSHOT.jar, we provides an executable row-counting program, `com.aliyun.openservices.tablestore.spark.RowCounter`.
