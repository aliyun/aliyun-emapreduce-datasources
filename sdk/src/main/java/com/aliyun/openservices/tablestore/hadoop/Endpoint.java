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

package com.aliyun.openservices.tablestore.hadoop;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.EOFException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class Endpoint implements Writable {
    public String endpoint;
    public String instance;

    private final Pattern kInstPattern = Pattern.compile("(http|https)://(.*?)[.].+");

    public Endpoint() {
    }
    public Endpoint(String endpoint) {
        this(endpoint, null);
    }
    public Endpoint(String endpoint, String instance) {
        Preconditions.checkNotNull(endpoint, "endpoint should not be null.");
        this.endpoint = endpoint;
        if (instance != null) {
            this.instance = instance;
        } else {
            Matcher m = kInstPattern.matcher(endpoint);
            Preconditions.checkArgument(
                m.matches(), "cannot parse instance from endpoint: " + endpoint);
            Preconditions.checkArgument(
                m.groupCount() == 2, "cannot parse instance from endpoint: " + endpoint);
            this.instance = m.group(2);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Endpoint)) {
            return false;
        }
        Endpoint a = (Endpoint) obj;
        if (!this.endpoint.equals(a.endpoint)) {
            return false;
        }
        if (!this.instance.equals(a.instance)) {
            return false;
        }
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(WritableConsts.ENDPOINT);
        out.writeUTF(endpoint);
        out.writeUTF(instance);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag != WritableConsts.ENDPOINT) {
            throw new IOException("broken input stream");
        }
        endpoint = null;
        instance = null;
        endpoint = in.readUTF();
        instance = in.readUTF();
    }

    public static Endpoint read(DataInput in) throws IOException {
        Endpoint w = new Endpoint();
        w.readFields(in);
        return w;
    }

    public String serialize() {
        return Utils.serialize(this);
    }

    public static Endpoint deserialize(String in) {
        if (in == null) {
            return null;
        }
        byte[] buf = Base64.decodeBase64(in);
        ByteArrayInputStream is = new ByteArrayInputStream(buf);
        DataInputStream din = new DataInputStream(is);
        try {
            return read(din);
        } catch(IOException ex) {
            return null;
        }
    }
}

