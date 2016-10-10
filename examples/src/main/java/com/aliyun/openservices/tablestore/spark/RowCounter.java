package com.aliyun.openservices.tablestore.spark;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Formatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.aliyun.openservices.tablestore.hadoop.Credential;
import com.aliyun.openservices.tablestore.hadoop.Endpoint;
import com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat;
import com.aliyun.openservices.tablestore.hadoop.PrimaryKeyWritable;
import com.aliyun.openservices.tablestore.hadoop.RowWritable;

public class RowCounter {
    private static String endpoint;
    private static String akId;
    private static String akSecret;
    private static String stsToken;
    private static String instance;
    private static String table;

    private static boolean parseArgs(String[] args) {
        for(int i = 0; i < args.length;) {
            if (args[i].equals("--endpoint")) {
                endpoint = args[i+1];
                i += 2;
            } else if (args[i].equals("--ak-id")) {
                akId = args[i+1];
                i += 2;
            } else if (args[i].equals("--ak-secret")) {
                akSecret = args[i+1];
                i += 2;
            } else if (args[i].equals("--instance")) {
                instance = args[i+1];
                i += 2;
            } else if (args[i].equals("--table")) {
                table = args[i+1];
                i += 2;
            } else if (args[i].equals("--sts")) {
                stsToken = args[i+1];
                i += 2;
            } else {
                return false;
            }
        }
        return true;
    }

    private static SyncClient getOTSClient() {
        Credential cred = new Credential(akId, akSecret, stsToken);
        Endpoint ep;
        if (instance == null) {
            ep = new Endpoint(endpoint);
        } else {
            ep = new Endpoint(endpoint, instance);
        }
        if (cred.stsToken == null) {
            return new SyncClient(ep.endpoint, cred.akId, cred.akSecret, ep.instance);
        } else {
            return new SyncClient(ep.endpoint, cred.akId, cred.akSecret, ep.instance,
                                  cred.stsToken);
        }
    }

    private static TableMeta fetchTableMeta() {
        SyncClient ots = getOTSClient();
        try {
            DescribeTableResponse resp = ots.describeTable(
                new DescribeTableRequest(table));
            return resp.getTableMeta();
        } finally {
            ots.shutdown();
        }
    }

    private static RangeRowQueryCriteria fetchCriteria() {
        TableMeta meta = fetchTableMeta();
        RangeRowQueryCriteria res = new RangeRowQueryCriteria(table);
        res.setMaxVersions(1);
        List<PrimaryKeyColumn> lower = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> upper = new ArrayList<PrimaryKeyColumn>();
        for(PrimaryKeySchema schema: meta.getPrimaryKeyList()) {
            lower.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MIN));
            upper.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MAX));
        }
        res.setInclusiveStartPrimaryKey(new PrimaryKey(lower));
        res.setExclusiveEndPrimaryKey(new PrimaryKey(upper));
        return res;
    }

    private static void printUsage() {
        System.err.println("Usage: RowCounter [options]");
        System.err.println("--ak-id\t\taccess-key id");
        System.err.println("--ak-secret\taccess-key secret");
        System.err.println("--sts\t(optional) STS token");
        System.err.println("--endpoint\tendpoint");
        System.err.println("--instance\tinstance name");
        System.err.println("--table\ttable name");
    }


    public static void main(String[] args) {
        if (!parseArgs(args)) {
            printUsage();
            System.exit(1);
        }
        if (endpoint == null ||
            akId == null ||
            akSecret == null ||
            table == null)
        {
            printUsage();
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("RowCounter");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Configuration hadoopConf = new Configuration();
        TableStoreInputFormat.setCredential(
            hadoopConf,
            new Credential(akId, akSecret, stsToken));
        Endpoint ep = (instance == null)
            ? new Endpoint(endpoint)
            : new Endpoint(endpoint, instance);
        TableStoreInputFormat.setEndpoint(hadoopConf, ep);
        TableStoreInputFormat.addCriteria(hadoopConf, fetchCriteria());

        try {
            JavaPairRDD<PrimaryKeyWritable, RowWritable> rdd = sc.newAPIHadoopRDD(
                hadoopConf,
                TableStoreInputFormat.class,
                PrimaryKeyWritable.class,
                RowWritable.class);
            System.out.println(
                new Formatter().format("TOTAL: %d", rdd.count()).toString());
        } finally {
            sc.close();
        }
    }
}
