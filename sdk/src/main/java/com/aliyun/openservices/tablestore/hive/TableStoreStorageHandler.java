package com.aliyun.openservices.tablestore.hive;

import java.util.Properties;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.plan.TableDesc;

public class TableStoreStorageHandler extends DefaultStorageHandler {
    private static Logger logger = LoggerFactory.getLogger(TableStoreStorageHandler.class);
    
    @Override
    public Class<? extends org.apache.hadoop.mapred.InputFormat> getInputFormatClass()
    {
        return TableStoreInputFormat.class;
    }

    @Override
    public Class<? extends org.apache.hadoop.mapred.OutputFormat> getOutputFormatClass()
    {
        return TableStoreOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass()
    {
        return TableStoreSerDe.class;
    }

    @Override
    public void configureInputJobProperties(
        TableDesc tableDesc,
        Map<String,String> jobProperties)
    {
        Properties props = tableDesc.getProperties();
        logger.debug("TableDesc: {}", props);
        for(String key: TableStoreConsts.REQUIRES) {
            String val = copyToMap(jobProperties, props, key);
            if (val == null) {
                logger.error("missing required table properties: {}", key);
                throw new IllegalArgumentException("missing required table properties: " + key);
            }
        }
        for(String key: TableStoreConsts.OPTIONALS) {
            copyToMap(jobProperties, props, key);
        }
    }

    private static String copyToMap(
        Map<String, String> to,
        Properties from,
        String key)
    {
        String val = from.getProperty(key);
        if (val != null) {
            to.put(key, val);
        }
        return val;
    }
    
    @Override
    public void configureOutputJobProperties(
        TableDesc tableDesc,
        Map<String,String> jobProperties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void configureTableJobProperties(
        TableDesc tableDesc,
        Map<String,String> jobProperties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void configureJobConf(
        TableDesc tableDesc,
        org.apache.hadoop.mapred.JobConf jobConf)
    {
        Properties from = tableDesc.getProperties();
        logger.debug("TableDesc: {}", from);
        logger.debug("job conf: {}", jobConf);
        jobConf.setJarByClass(TableStoreStorageHandler.class);
        {
            com.aliyun.openservices.tablestore.hadoop.Credential cred =
                new com.aliyun.openservices.tablestore.hadoop.Credential(
                    from.getProperty(TableStoreConsts.ACCESS_KEY_ID),
                    from.getProperty(TableStoreConsts.ACCESS_KEY_SECRET),
                    from.getProperty(TableStoreConsts.STS_TOKEN));
            com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .setCredential(jobConf, cred);
        }
        {
            String endpoint = from.getProperty(TableStoreConsts.ENDPOINT);
            String instance = from.getProperty(TableStoreConsts.INSTANCE);
            com.aliyun.openservices.tablestore.hadoop.Endpoint ep;
            if (instance == null) {
                ep = new com.aliyun.openservices.tablestore.hadoop.Endpoint(
                    endpoint);
            } else {
                ep = new com.aliyun.openservices.tablestore.hadoop.Endpoint(
                    endpoint, instance);
            }
            com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .setEndpoint(jobConf, ep);
        }
   }
}
