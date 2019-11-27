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

        @Override public void map(PrimaryKeyWritable key, RowWritable value, 
            Context context) throws IOException, InterruptedException {
            context.write(agg, one);
        }
    }

    public static class IntSumReducer
      extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override public void reduce(Text key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
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
    job.setJarByClass(RowCounter.class);
    job.setMapperClass(RowCounterMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setInputFormatClass(TableStoreInputFormat.class);

    TableStore.setCredential(job, accessKeyId, accessKeySecret, securityToken);
    TableStore.setEndpoint(job, endpoint, instance);
    TableStoreInputFormat.addCriteria(job, fetchCriteria());
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
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
$ HADOOP_CLASSPATH=emr-tablestore-2.0.0-SNAPSHOT.jar:tablestore-4.1.0-jar-with-dependencies.jar::joda-time-2.9.4.jar:YourRowCounter.jar bin/hadoop YourRowCounterClass
...
$ find out -type f
out/_SUCCESS
out/part-r-00000
out/._SUCCESS.crc
out/.part-r-00000.crc
$ cat out/part-r-00000
TOTAL   9
```

FYI, in emr-examples_2.11-2.0.0-SNAPSHOT.jar, we provides an executable row-counting program, `com.aliyun.openservices.tablestore.hadoop.RowCounter`.

## TableStore as Data Sink

### Prepare tables

Now, we will save results from map-reduce into an existing TableStore table.
This time, we are interested in mapping owners to their pets.
Besides `pet` table defined in the previous section, we have to create an empty `owner_pets` table, whose primary key is only a string column named by `owner`.

### Owners and their pets

We are going to group pets by their owners.
Precisely speaking, in each row, the primary key is owner's name and each attribute column of this row is a pet, whose name and value are name and species of the pet respectively.

```java
public static class OwnerMapper
  extends Mapper<PrimaryKeyWritable, RowWritable, Text, MapWritable> {
    @Override public void map(PrimaryKeyWritable key, RowWritable row, 
        Context context) throws IOException, InterruptedException {
        PrimaryKeyColumn pet = key.getPrimaryKey().getPrimaryKeyColumn("name");
        Column owner = row.getRow().getLatestColumn("owner");
        Column species = row.getRow().getLatestColumn("species");
        MapWritable m = new MapWritable();
        m.put(new Text(pet.getValue().asString()),
            new Text(species.getValue().asString()));
        context.write(new Text(owner.getValue().asString()), m);
    }
}

public static class IntoTableReducer
  extends Reducer<Text,MapWritable,Text,BatchWriteWritable> {

    @Override public void reduce(Text owner, Iterable<MapWritable> pets, 
        Context context) throws IOException, InterruptedException {
        List<PrimaryKeyColumn> pkeyCols = new ArrayList<PrimaryKeyColumn>();
        pkeyCols.add(new PrimaryKeyColumn("owner",
                PrimaryKeyValue.fromString(owner.toString())));
        PrimaryKey pkey = new PrimaryKey(pkeyCols);
        List<Column> attrs = new ArrayList<Column>();
        for(MapWritable petMap: pets) {
            for(Map.Entry<Writable, Writable> pet: petMap.entrySet()) {
                Text name = (Text) pet.getKey();
                Text species = (Text) pet.getValue();
                attrs.add(new Column(name.toString(),
                        ColumnValue.fromString(species.toString())));
            }
        }
        RowPutChange putRow = new RowPutChange(outputTable, pkey)
            .addColumns(attrs);
        BatchWriteWritable batch = new BatchWriteWritable();
        batch.addRowChange(putRow);
        context.write(owner, batch);
    }
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, TableStoreOutputFormatExample.class.getName());
    job.setMapperClass(OwnerMapper.class);
    job.setReducerClass(IntoTableReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    job.setInputFormatClass(TableStoreInputFormat.class);
    job.setOutputFormatClass(TableStoreOutputFormat.class);

    TableStore.setCredential(job, accessKeyId, accessKeySecret, securityToken);
    TableStore.setEndpoint(job, endpoint, instance);
    TableStoreInputFormat.addCriteria(job, ...);
    TableStoreOutputFormat.setOutputTable(job, outputTable);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
```

In `main()`, `TableStoreOutputFormat` is specified as the data sink by `job.setOutputFormatClass()`.
`TableStoreOutputformat` requires endpoint, credential and a output table.
They are set by `TableStoreInputFormat.setCredential()`, `TableStoreInputFormat.setEndpoint` and `TableStoreOutputFormat.setOutputTable()`.

In mappers, we extract name, species and owner of each pet,
and group mappings from pet names to their species by their owners.

In reducers, we translate each owner and their pets into a `RowPutChange` which is defined in JAVA SDK for TableStore, 
and then wrap this `RowPutChange` into a `BatchWriteWritable`.
A `BatchWriteWritable` is able to contain zero or many `RowPutChange`s as well as `RowUpdateChange`s and `RowDeleteChange`s, which are also defined in JAVA SDK for TableStore.
When a `RowPutChange` is applied, the entire row is replaced;
`RowDeleteChange`, the entire row is deleted.
One can update part of attribute columns or delete part of attribute columns by `RowUpdateChange`s.
Please refer to documents of JAVA SDK for TableStore for details.

Let us have a try.
If everything okey, `owner_pets` looks like

| owner | Fang | Slim  | Bowser | Puffball | Chirpy | Claws | Whistler | Buffy | Fluffy |
|-------|------|-------|--------|----------|--------|-------|----------|-------|--------|
| Benny | dog  | snake |        |          |        |       |          |       |        |
| Diane |      |       | dog    | hamster  |        |       |          |       |        |
| Gwen  |      |       |        |          | bird   | cat   | bird     |       |        |
| Harold|      |       |        |          |        |       |          | dog   | cat    |