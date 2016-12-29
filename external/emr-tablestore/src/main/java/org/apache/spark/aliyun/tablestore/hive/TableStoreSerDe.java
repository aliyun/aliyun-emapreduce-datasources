/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.aliyun.tablestore.hive;

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
import java.util.Set;
import java.util.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import org.apache.spark.aliyun.tablestore.hadoop.TableStore;
import org.apache.spark.aliyun.tablestore.hadoop.RowWritable;
import org.apache.spark.aliyun.tablestore.hadoop.BatchWriteWritable;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStoreSerDe extends AbstractSerDe {
    private static Logger logger = LoggerFactory.getLogger(TableStoreSerDe.class);
    private ArrayList<TypeInfo> columnTypes;
    private List<String> columnNames;
    private ObjectInspector inspector;
    private Configuration config;
    private Properties tableProperties;
    private TableMeta tableMeta;

    @Override public void initialize(Configuration conf, Properties tableProps,
        Properties partProps) throws SerDeException {
        initialize(conf, tableProps);
    }

    @Override public void initialize(Configuration conf, Properties props)
        throws SerDeException {
        logger.debug("table properties: {}", props);
        if (!"TRUE".equals(props.getProperty("EXTERNAL"))) {
            logger.error("only external table are supported so far: {}", props.getProperty("EXTERNAL"));
            throw new SerDeException("only external table are supported so far");
        }

        this.config = conf;
        this.tableProperties = props;
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

    @Override public Class<? extends org.apache.hadoop.io.Writable> getSerializedClass() {
        return BatchWriteWritable.class;
    }

    @Override public org.apache.hadoop.io.Writable serialize(Object obj,
        ObjectInspector objInspector) throws SerDeException {
        if (objInspector.getCategory() != ObjectInspector.Category.STRUCT) {
            throw new SerDeException(getClass().toString()
                + " can only serialize struct types, but we got: "
                + objInspector.getTypeName());
        }

        StructObjectInspector soi = (StructObjectInspector) objInspector;
        List<? extends StructField> fieldMetas = soi.getAllStructFieldRefs();
        List<Object> values = soi.getStructFieldsDataAsList(obj);
        assert fieldMetas.size() == values.size();
        assert this.columnNames.size() == values.size();
        Map<String, Field> fields = new HashMap<String, Field>();
        for(int i = 0; i < values.size(); i++) {
            String name = this.columnNames.get(i);
            Field field = new Field();
            field.name = name;
            field.meta = fieldMetas.get(i);
            field.value = values.get(i);
            fields.put(name, field);
        }

        TableMeta tableMeta = fetchTableMeta();
        Set<String> pkeyNames = new HashSet<String>();
        List<PrimaryKeyColumn> pkeyCols = new ArrayList<PrimaryKeyColumn>();
        for(PrimaryKeySchema s: tableMeta.getPrimaryKeyList()) {
            String name = s.getName();
            pkeyNames.add(name);
            ColumnValue value = toColumnValue(fields.get(name));
            if (value == null) {
                throw new SerDeException(getClass().getName()
                    + " cannot write to table "
                    + this.tableProperties.get(TableStoreConsts.TABLE_NAME)
                    + " in TableStore, because primary-key column "
                    + name
                    + " is required.");
            }
            pkeyCols.add(new PrimaryKeyColumn(name, PrimaryKeyValue.fromColumn(value)));
        }
        RowUpdateChange row = new RowUpdateChange(
            tableMeta.getTableName(),
            new PrimaryKey(pkeyCols));
        for(Field field: fields.values()) {
            String name = field.name;
            if (pkeyNames.contains(name)) {
                continue;
            }
            ColumnValue colVal = toColumnValue(field);
            if (colVal == null) {
                row.deleteColumns(name);
            } else {
                row.put(name, colVal);
            }
        }
        BatchWriteWritable batch = new BatchWriteWritable();
        batch.addRowChange(row);
        return batch;
    }

    @Override public SerDeStats getSerDeStats() {
        return null;
    }

    @Override public Object deserialize(org.apache.hadoop.io.Writable blob) 
        throws SerDeException {
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

    @Override public ObjectInspector getObjectInspector() throws SerDeException {
        return this.inspector;
    }

    /**
     * for testing only
     */
    public List<TypeInfo> getColumnTypes() {
        return this.columnTypes;
    }

    /**
     * for testing only
     */
    public List<String> getColumnNames() {
        return this.columnNames;
    }

    private static Object extractHiveValue(PrimaryKey pkey,
        Map<String, NavigableMap<Long, ColumnValue>> attrs, String name,
        PrimitiveTypeInfo type) throws SerDeException {
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
                    logger.error("data type mismatch. row: {}, column: {}, expect: {} real: {}",
                        pkey, name, type.getPrimitiveCategory(), val.getType());
                    throw new SerDeException("data type mismatch");
                }
            case STRING:
                switch(type.getPrimitiveCategory()) {
                case STRING:
                    return val.asString();
                default:
                    logger.error("data type mismatch. row: {}, column: {}, expect: {} real: {}",
                        pkey, name, type.getPrimitiveCategory(), val.getType());
                    throw new SerDeException("data type mismatch");
                }
            case BINARY:
                switch(type.getPrimitiveCategory()) {
                case BINARY:
                    return val.asBinary();
                default:
                    logger.error("data type mismatch. row: {}, column: {}, expect: {} real: {}",
                        pkey, name, type.getPrimitiveCategory(), val.getType());
                    throw new SerDeException("data type mismatch");
                }
            default:
                logger.error("unknown data type on deserializing. row: {}, column: {}, value: {}",
                    pkey, name, pkCol);
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
                        logger.error("data type mismatch. row: {}, column: {}, expect: {} real: {}",
                            pkey, name, type.getPrimitiveCategory(), val.getType());
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
                        logger.error("data type mismatch. row: {}, column: {}, expect: {} real: {}",
                            pkey, name, type.getPrimitiveCategory(), val.getType());
                        throw new SerDeException("data type mismatch");
                    }
                case STRING:
                    switch(type.getPrimitiveCategory()) {
                    case STRING:
                        return val.asString();
                    default:
                        logger.error("data type mismatch. row: {}, column: {}, expect: {} real: {}",
                            pkey, name, type.getPrimitiveCategory(), val.getType());
                        throw new SerDeException("data type mismatch");
                    }
                case BINARY:
                    switch(type.getPrimitiveCategory()) {
                    case BINARY:
                        return val.asBinary();
                    default:
                        logger.error("data type mismatch. row: {}, column: {}, expect: {} real: {}",
                            pkey, name, type.getPrimitiveCategory(), val.getType());
                        throw new SerDeException("data type mismatch");
                    }
                case BOOLEAN:
                    switch(type.getPrimitiveCategory()) {
                    case BOOLEAN:
                        return val.asBoolean();
                    default:
                        logger.error("data type mismatch. row: {}, column: {}, expect: {} real: {}",
                            pkey, name, type.getPrimitiveCategory(), val.getType());
                        throw new SerDeException("data type mismatch");
                    }
                default:
                    logger.error("unknown data type on deserializing. row: {}, column: {}, value: {}",
                        pkey, name, val);
                    throw new SerDeException("unknown data type of primary key");
                }
            } else {
                return null;
            }
        }
    }

    private TableMeta fetchTableMeta() {
        if (tableMeta == null) {
            SyncClientInterface ots = TableStore.newOtsClient(this.config);
            try {
                DescribeTableRequest req = new DescribeTableRequest(
                    tableProperties.getProperty(TableStoreConsts.TABLE_NAME));
                DescribeTableResponse resp = ots.describeTable(req);
                tableMeta = resp.getTableMeta();
            } finally {
                ots.shutdown();
            }
        }
        return tableMeta;
    }

    private class Field {
        public String name;
        public StructField meta;
        public Object value;
    }
    
    private ColumnValue toColumnValue(Field field) throws SerDeException {
        Preconditions.checkNotNull(field, "field must be nonnull.");
        ObjectInspector insp = field.meta.getFieldObjectInspector();
        Preconditions.checkNotNull(
            insp.getCategory() == ObjectInspector.Category.PRIMITIVE,
            "field must be primitive.");
        PrimitiveObjectInspector priInsp = (PrimitiveObjectInspector) insp;
        Object obj = priInsp.getPrimitiveJavaObject(field.value);
        if (obj == null) {
            return null;
        }
        switch(priInsp.getPrimitiveCategory()) {
        case BYTE: case SHORT: case INT: case LONG: {
            return ColumnValue.fromLong(((Number) obj).longValue());
        }
        case DECIMAL: case FLOAT: case DOUBLE: {
            return ColumnValue.fromDouble(((Number) obj).doubleValue());
        }
        case BOOLEAN: {
            return ColumnValue.fromBoolean(((Boolean) obj).booleanValue());
        }
        case CHAR: case VARCHAR: case STRING: {
            return ColumnValue.fromString((String) obj);
        }
        case BINARY: {
            return ColumnValue.fromBinary((byte[]) obj);
        }
        default: {
            logger.error(
                "TableStore column name: {}, JAVA type: {}, Hive type: {}",
                field.name, obj.getClass().getName(), priInsp.getPrimitiveCategory());
            throw new SerDeException(
                "Unsupported Hive type: " + priInsp.getPrimitiveCategory());
        }
        }
    }
}
