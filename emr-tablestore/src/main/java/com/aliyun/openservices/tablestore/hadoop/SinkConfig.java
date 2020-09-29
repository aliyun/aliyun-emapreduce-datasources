/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.openservices.tablestore.hadoop;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;

import java.io.*;

public class SinkConfig implements Writable {
    private String version;

    private String writerBatchRequestType;
    private boolean writerBatchOrderGuaranteed;
    private boolean writerBatchDuplicateAllowed;
    private String writerRowChangeType;

    private int writerBucketNum;
    private int writerCallbackPoolNum;
    private int writerCallbackPoolQueueSize;
    private int writerConcurrency;
    private int writerBufferSize;
    private int writerFlushIntervalMs;
    private int clientIoPool;

    private int writerMaxBatchSize;
    private int writerMaxBatchCount;
    private int writerMaxColumnCount;
    private int writerMaxAttrSize;
    private int writerMaxPkSize;
    private String clientRetryStrategy;
    private int clientRetryTime;
    private int clientRetryCount;
    private int clientRetryPause;

    private boolean ignoreOnFailureEnabled;

    public SinkConfig() {
    }

    public SinkConfig(String version,
                      String writerBatchRequestType, boolean writerBatchOrderGuaranteed,
                      boolean writerBatchDuplicateAllowed, String writerRowChangeType,
                      int writerBucketNum, int writerCallbackPoolNum, int writerCallbackPoolQueueSize,
                      int writerConcurrency, int writerBufferSize, int writerFlushIntervalMs, int clientIoPool,
                      int writerMaxBatchSize, int writerMaxBatchCount, int writerMaxColumnCount, int writerMaxAttrSize,
                      int writerMaxPkSize,
                      String clientRetryStrategy, int clientRetryTime, int clientRetryCount, int clientRetryPause,
                      boolean ignoreOnFailureEnabled) {
        this.version = version;
        this.writerBatchRequestType = writerBatchRequestType;
        this.writerBatchOrderGuaranteed = writerBatchOrderGuaranteed;
        this.writerBatchDuplicateAllowed = writerBatchDuplicateAllowed;
        this.writerRowChangeType = writerRowChangeType;
        this.writerBucketNum = writerBucketNum;
        this.writerCallbackPoolNum = writerCallbackPoolNum;
        this.writerCallbackPoolQueueSize = writerCallbackPoolQueueSize;
        this.writerConcurrency = writerConcurrency;
        this.writerBufferSize = writerBufferSize;
        this.writerFlushIntervalMs = writerFlushIntervalMs;
        this.clientIoPool = clientIoPool;
        this.writerMaxBatchSize = writerMaxBatchSize;
        this.writerMaxBatchCount = writerMaxBatchCount;
        this.writerMaxColumnCount = writerMaxColumnCount;
        this.writerMaxAttrSize = writerMaxAttrSize;
        this.writerMaxPkSize = writerMaxPkSize;
        this.clientRetryStrategy = clientRetryStrategy;
        this.clientRetryTime = clientRetryTime;
        this.clientRetryCount = clientRetryCount;
        this.clientRetryPause = clientRetryPause;
        this.ignoreOnFailureEnabled = ignoreOnFailureEnabled;
    }

    public String getVersion() {
        return version;
    }

    public String getWriterBatchRequestType() {
        return writerBatchRequestType;
    }

    public boolean isWriterBatchOrderGuaranteed() {
        return writerBatchOrderGuaranteed;
    }

    public boolean isWriterBatchDuplicateAllowed() {
        return writerBatchDuplicateAllowed;
    }

    public String getWriterRowChangeType() {
        return writerRowChangeType;
    }

    public int getWriterBucketNum() {
        return writerBucketNum;
    }

    public int getWriterCallbackPoolNum() {
        return writerCallbackPoolNum;
    }

    public int getWriterCallbackPoolQueueSize() {
        return writerCallbackPoolQueueSize;
    }

    public int getWriterConcurrency() {
        return writerConcurrency;
    }

    public int getWriterBufferSize() {
        return writerBufferSize;
    }

    public int getWriterFlushIntervalMs() {
        return writerFlushIntervalMs;
    }

    public int getClientIoPool() {
        return clientIoPool;
    }

    public int getWriterMaxBatchSize() {
        return writerMaxBatchSize;
    }

    public int getWriterMaxBatchCount() {
        return writerMaxBatchCount;
    }

    public int getWriterMaxColumnCount() {
        return writerMaxColumnCount;
    }

    public int getWriterMaxAttrSize() {
        return writerMaxAttrSize;
    }

    public int getWriterMaxPkSize() {
        return writerMaxPkSize;
    }

    public String getClientRetryStrategy() {
        return clientRetryStrategy;
    }

    public int getClientRetryTime() {
        return clientRetryTime;
    }

    public int getClientRetryCount() {
        return clientRetryCount;
    }

    public int getClientRetryPause() {
        return clientRetryPause;
    }

    public boolean isIgnoreOnFailureEnabled() {
        return ignoreOnFailureEnabled;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(WritableConsts.SINK_PARAMETERS);
        dataOutput.writeUTF(version);
        dataOutput.writeUTF(writerBatchRequestType);
        dataOutput.writeBoolean(writerBatchOrderGuaranteed);
        dataOutput.writeBoolean(writerBatchDuplicateAllowed);
        dataOutput.writeUTF(writerRowChangeType);
        dataOutput.writeInt(writerBucketNum);
        dataOutput.writeInt(writerCallbackPoolNum);
        dataOutput.writeInt(writerCallbackPoolQueueSize);
        dataOutput.writeInt(writerConcurrency);
        dataOutput.writeInt(writerBufferSize);
        dataOutput.writeInt(writerFlushIntervalMs);
        dataOutput.writeInt(clientIoPool);
        dataOutput.writeInt(writerMaxBatchSize);
        dataOutput.writeInt(writerMaxBatchCount);
        dataOutput.writeInt(writerMaxColumnCount);
        dataOutput.writeInt(writerMaxAttrSize);
        dataOutput.writeInt(writerMaxPkSize);
        dataOutput.writeUTF(clientRetryStrategy);
        dataOutput.writeInt(clientRetryTime);
        dataOutput.writeInt(clientRetryCount);
        dataOutput.writeInt(clientRetryPause);
        dataOutput.writeBoolean(ignoreOnFailureEnabled);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        byte tag = dataInput.readByte();
        if (tag != WritableConsts.SINK_PARAMETERS) {
            throw new IOException("broken input stream");
        }
        version = dataInput.readUTF();
        writerBatchRequestType = dataInput.readUTF();
        writerBatchOrderGuaranteed = dataInput.readBoolean();
        writerBatchDuplicateAllowed = dataInput.readBoolean();
        writerRowChangeType = dataInput.readUTF();
        writerBucketNum = dataInput.readInt();
        writerCallbackPoolNum = dataInput.readInt();
        writerCallbackPoolQueueSize = dataInput.readInt();
        writerConcurrency = dataInput.readInt();
        writerBufferSize = dataInput.readInt();
        writerFlushIntervalMs = dataInput.readInt();
        clientIoPool = dataInput.readInt();
        writerMaxBatchSize = dataInput.readInt();
        writerMaxBatchCount = dataInput.readInt();
        writerMaxColumnCount = dataInput.readInt();
        writerMaxAttrSize = dataInput.readInt();
        writerMaxPkSize = dataInput.readInt();
        clientRetryStrategy = dataInput.readUTF();
        clientRetryTime = dataInput.readInt();
        clientRetryCount = dataInput.readInt();
        clientRetryPause = dataInput.readInt();
        ignoreOnFailureEnabled = dataInput.readBoolean();
    }

    public String serialize() {
        return Utils.serialize(this);
    }

    public static SinkConfig deserialize(String in) {
        if (in == null) {
            return null;
        }
        byte[] buf = Base64.decodeBase64(in);
        ByteArrayInputStream is = new ByteArrayInputStream(buf);
        DataInputStream din = new DataInputStream(is);
        try {
            SinkConfig sinkConfig = new SinkConfig();
            sinkConfig.readFields(din);
            return sinkConfig;
        } catch (IOException ex) {
            return null;
        }
    }

}
