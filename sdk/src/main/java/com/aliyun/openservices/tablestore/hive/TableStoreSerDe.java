package com.aliyun.openservices.tablestore.hive;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.IOException;

import java.util.Properties;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import com.aliyun.openservices.tablestore.hadoop.RowWritable;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStoreSerDe extends AbstractSerDe {
    private static Logger logger = LoggerFactory.getLogger(TableStoreSerDe.class);
    private ArrayList<TypeInfo> columnTypes;
    private List<String> columnNames;
    private ObjectInspector inspector;

    @Override
    public void initialize(
        Configuration conf,
        Properties tableProps,
        Properties partProps)
        throws SerDeException
    {
        initialize(conf, tableProps);
    }

    @Override
    public void initialize(
        org.apache.hadoop.conf.Configuration conf,
        Properties props)
        throws SerDeException
    {
        logger.debug("table properties: {}", props);
        if (!"TRUE".equals(props.getProperty("EXTERNAL"))) {
            logger.error("only external table are supported so far");
            throw new SerDeException("only external table are supported so far");
        }
        
        this.columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(
            props.getProperty(serdeConstants.LIST_COLUMN_TYPES));

        String mapping = props.getProperty(TableStoreConsts.COLUMNS_MAPPING);
        if (mapping == null) {
            mapping = props.getProperty(serdeConstants.LIST_COLUMNS);
        }
        this.columnNames = Arrays.asList(mapping.split(","));
        if (this.columnTypes.size() != this.columnNames.size()) {
            logger.error("# of column names is different with # of column types, {} {}",
                this.columnNames.size(),
                this.columnTypes.size());
            throw new SerDeException("# of column names is different with # of column types");
        }
        for(int i = 0, sz = this.columnTypes.size(); i < sz; ++i) {
            TypeInfo type = this.columnTypes.get(i);
            if (!(type instanceof PrimitiveTypeInfo)) {
                logger.error("type of column {} is {}, but accepts only primitive types",
                    this.columnNames.get(i),
                    type.getCategory());
                throw new SerDeException("only primitive types are supported");
            }
        }
        this.inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
            (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(
                Arrays.asList(
                    props.getProperty(serdeConstants.LIST_COLUMNS).split(",")),
                this.columnTypes));
    }

    @Override
    public Class<? extends org.apache.hadoop.io.Writable> getSerializedClass()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.hadoop.io.Writable serialize(
        Object obj,
        ObjectInspector objInspector)
        throws SerDeException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SerDeStats getSerDeStats()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object deserialize(org.apache.hadoop.io.Writable blob)
        throws SerDeException
    {
        if (!(blob instanceof RowWritable)) {
            logger.error("deserialization expects {} but {} is given: {}",
                RowWritable.class.getName(),
                blob.getClass().getName(),
                blob);
            throw new SerDeException("fail to deserialize");
        }
        Row row = ((RowWritable) blob).getRow();
        logger.debug("deserializing {}", row);

        List<Object> res = new ArrayList<Object>(this.columnTypes.size());

        PrimaryKey pkey = row.getPrimaryKey();
        Map<String, NavigableMap<Long, ColumnValue>> attrs = row.getColumnsMap();
        for(int i = 0, sz = this.columnNames.size(); i < sz; ++i) {
            String name = this.columnNames.get(i);
            PrimitiveTypeInfo type = (PrimitiveTypeInfo) this.columnTypes.get(i);
            res.add(extractHiveValue(pkey, attrs, name, type));
        }

        logger.debug("deserialized {}", row);
        return res;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException
    {
        return this.inspector;
    }

    /**
     * for testing only
     */
    public List<TypeInfo> getColumnTypes()
    {
        return this.columnTypes;
    }

    /**
     * for testing only
     */
    public List<String> getColumnNames()
    {
        return this.columnNames;
    }

    private static Object extractHiveValue(
        PrimaryKey pkey,
        Map<String, NavigableMap<Long, ColumnValue>> attrs,
        String name,
        PrimitiveTypeInfo type)
        throws SerDeException
    {
        PrimaryKeyColumn pkCol = pkey.getPrimaryKeyColumn(name);
        if (pkCol != null) {
            PrimaryKeyValue val = pkCol.getValue();
            switch(val.getType()) {
            case INTEGER:
                switch(type.getPrimitiveCategory()) {
                case LONG:
                    return val.asLong();
                case INT:
                    return (int) val.asLong();
                case SHORT:
                    return (short) val.asLong();
                case BYTE:
                    return (byte) val.asLong();
                case DOUBLE:
                    return (double) val.asLong();
                case FLOAT:
                    return (float) val.asLong();
                default:
                    logger.error("data type mismatch. column: {}, expect: {} real: {}",
                        name,
                        type.getPrimitiveCategory(),
                        val.getType());
                    throw new SerDeException("data type mismatch");
                }
            case STRING:
                switch(type.getPrimitiveCategory()) {
                case STRING:
                    return val.asString();
                default:
                    logger.error("data type mismatch. column: {}, expect: {} real: {}",
                        name,
                        type.getPrimitiveCategory(),
                        val.getType());
                    throw new SerDeException("data type mismatch");
                }
            case BINARY:
                switch(type.getPrimitiveCategory()) {
                case BINARY:
                    return val.asBinary();
                default:
                    logger.error("data type mismatch. column: {}, expect: {} real: {}",
                        name,
                        type.getPrimitiveCategory(),
                        val.getType());
                    throw new SerDeException("data type mismatch");
                }
            default:
                logger.error("unknown data type on deserializing. column: {}, value: {}",
                    name,
                    pkCol);
                throw new SerDeException("unknown data type of primary key");
            }
        } else {
            NavigableMap<Long, ColumnValue> values = attrs.get(name);
            if (values != null) {
                Preconditions.checkArgument(values.size() == 1,
                    "only the latest version should be fetched");
                ColumnValue val = values.lastEntry().getValue();
                switch(val.getType()) {
                case INTEGER:
                    switch(type.getPrimitiveCategory()) {
                    case LONG:
                        return val.asLong();
                    case INT:
                        return (int) val.asLong();
                    case SHORT:
                        return (short) val.asLong();
                    case BYTE:
                        return (byte) val.asLong();
                    case DOUBLE:
                        return (double) val.asLong();
                    case FLOAT:
                        return (float) val.asLong();
                    default:
                        logger.error("data type mismatch. column: {}, expect: {} real: {}",
                            name,
                            type.getPrimitiveCategory(),
                            val.getType());
                        throw new SerDeException("data type mismatch");
                    }
                case DOUBLE:
                    switch(type.getPrimitiveCategory()) {
                    case LONG:
                        return (long) val.asDouble();
                    case INT:
                        return (int) val.asDouble();
                    case SHORT:
                        return (short) val.asDouble();
                    case BYTE:
                        return (byte) val.asDouble();
                    case DOUBLE:
                        return val.asDouble();
                    case FLOAT:
                        return (float) val.asDouble();
                    default:
                        logger.error("data type mismatch. column: {}, expect: {} real: {}",
                            name,
                            type.getPrimitiveCategory(),
                            val.getType());
                        throw new SerDeException("data type mismatch");
                    }
                case STRING:
                    switch(type.getPrimitiveCategory()) {
                    case STRING:
                        return val.asString();
                    default:
                        logger.error("data type mismatch. column: {}, expect: {} real: {}",
                            name,
                            type.getPrimitiveCategory(),
                            val.getType());
                        throw new SerDeException("data type mismatch");
                    }
                case BINARY:
                    switch(type.getPrimitiveCategory()) {
                    case BINARY:
                        return val.asBinary();
                    default:
                        logger.error("data type mismatch. column: {}, expect: {} real: {}",
                            name,
                            type.getPrimitiveCategory(),
                            val.getType());
                        throw new SerDeException("data type mismatch");
                    }
                case BOOLEAN:
                    switch(type.getPrimitiveCategory()) {
                    case BOOLEAN:
                        return val.asBoolean();
                    default:
                        logger.error("data type mismatch. column: {}, expect: {} real: {}",
                            name,
                            type.getPrimitiveCategory(),
                            val.getType());
                        throw new SerDeException("data type mismatch");
                    }
                default:
                    logger.error("unknown data type on deserializing. column: {}, value: {}",
                        name,
                        val);
                    throw new SerDeException("unknown data type of primary key");
                }
            } else {
                return null;
            }
        }
    }
}
