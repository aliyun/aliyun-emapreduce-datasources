# HadoopMR on TableStore

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

First, we need mappers and reducers.

```java
public class RowCounter {
    public static class RowCounterMapper
      extends Mapper<PrimaryKeyWritable, RowWritable, Text, LongWritable> {
        private final static Text agg = new Text("TOTAL");
        private final static LongWritable one = new LongWritable(1);

        @Override
        public void map(
            PrimaryKeyWritable key, RowWritable value, Context context)
            throws IOException, InterruptedException {
            context.write(agg, one);
        }
    }

    public static class IntSumReducer
      extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        public void reduce(
            Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
}
```

Each time HadoopMR fetches a row from `pet`, it will invoke `map()` of the mapper.
The first two arguments, `PrimaryKeyWritable` and `RowWritable`, are wrappers to `PrimaryKey` and `Row` in [JAVA SDK for TableStore](https://help.aliyun.com/document_detail/43005.html?spm=5176.doc43009.6.203.pUutFA).

For counting rows, we do not take care of details of `PrimaryKey` and `Row`.
We just `+1`.
And, a note: because TableStore can easily hold trillions of rows, `int` is not large enough.

Second, allow me to show how to config TableStore as data source.

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

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "row count");
    job.setJarByClass(RowCounter.class);
    job.setMapperClass(RowCounterMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setInputFormatClass(TableStoreInputFormat.class);
    TableStoreInputFormat.setEndpoint(job, "YourEndpoint");
    TableStoreInputFormat.setCredential(job, "YourAccessKeyId", "YourAccessKeySecret");
    TableStoreInputFormat.addCriteria(job, fetchCriteria());
    FileOutputFormat.setOutputPath(job, new Path("out"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
```

Essentially, `job.setInputFormatClass(TableStoreInputFormat.class)` sets TableStore as data source.
Besides this statement, we have to let `TableStoreInputFormat` know who wants to access which tables.
* `TableStoreInputFormat.setEndpoint()` sets the [endpoint](https://help.aliyun.com/document_detail/27285.html?spm=5176.doc27281.6.109.8I57sZ).
* `TableStoreInputFormat.setCredential()` sets the [credential](https://help.aliyun.com/document_detail/27296.html?spm=5176.doc27360.6.136.3FDlzl).
  + `setCredential()` supports [Security Token](https://help.aliyun.com/document_detail/27360.html?spm=5176.doc27296.6.236.4AFNPp) as well.
* `TableStoreInputFormat.addCriteria()` can be invoked many times,
  each time adds a [`RangeRowQueryCriteria`](https://help.aliyun.com/document_detail/43017.html?spm=5176.doc27360.6.208.RgVtBA) object.
  + We can set [filters](https://help.aliyun.com/document_detail/43029.html?spm=5176.doc43017.6.210.98IWE3) and columns-to-get to filter rows and columns in server-side.
  + By adding `RangeRowQueryCriteria`s over multiple tables, we can easily union them.
  + By adding `RangeRowQueryCriteria`s over a single table, we can tune splits.
    `TableStoreInputFormat` will heuristically split table data.
    However, sometimes we known better than the algorithm.
    Then we can tell it by adding a series of `RangeRowQueryCriteria`s.

OK, let the program run.

```
$ rm -rf out
$ HADOOP_CLASSPATH=emr-sdk_2.10-1.3.1.jar:tablestore-4.1.0-jar-with-dependencies.jar::joda-time-2.9.4.jar:YourRowCounter.jar bin/hadoop YourRowCounterClass
...
$ find out -type f
out/_SUCCESS
out/part-r-00000
out/._SUCCESS.crc
out/.part-r-00000.crc
$ cat out/part-r-00000
TOTAL   9
```

FYI, in emr-examples_2.10-1.3.1.jar, we provides an executable row-counting program, `com.aliyun.openservices.tablestore.hadoop.RowCounter`.
