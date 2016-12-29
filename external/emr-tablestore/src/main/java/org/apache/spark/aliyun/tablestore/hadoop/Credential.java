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

package org.apache.spark.aliyun.tablestore.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.EOFException;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class Credential implements Writable {
    public String accessKeyId;
    public String accessKeySecret;
    public String securityToken;

    public Credential() {
    }

    public Credential(String accessKeyId, String accessKeySecret, String securityToken) {
        Preconditions.checkNotNull(accessKeyId, "accessKeyId should not be null.");
        Preconditions.checkNotNull(accessKeySecret, "accessKeySecret should not be null.");
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.securityToken = securityToken;
    }

    @Override public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Credential)) {
            return false;
        }
        Credential a = (Credential) obj;
        if (!this.accessKeyId.equals(a.accessKeyId)) {
            return false;
        }
        if (!this.accessKeySecret.equals(a.accessKeySecret)) {
            return false;
        }
        if (this.securityToken == null) {
            return a.securityToken == null;
        } else {
            return this.securityToken.equals(a.securityToken);
        }
    }

    @Override public void write(DataOutput out) throws IOException {
        out.writeByte(WritableConsts.CREDENTIAL);
        out.writeUTF(accessKeyId);
        out.writeUTF(accessKeySecret);
        if (securityToken != null) {
            out.writeByte(WritableConsts.CREDENTIAL_SECURITY_TOKEN);
            out.writeUTF(securityToken);
        }
    }

    @Override public void readFields(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag != WritableConsts.CREDENTIAL) {
            throw new IOException("broken input stream");
        }
        accessKeyId = null;
        accessKeySecret = null;
        securityToken = null;
        accessKeyId = in.readUTF();
        accessKeySecret = in.readUTF();
        byte tagSecurityToken = nextTag(in);
        if (tagSecurityToken == WritableConsts.CREDENTIAL_SECURITY_TOKEN) {
            securityToken = in.readUTF();
        }
    }

    public static Credential read(DataInput in) throws IOException {
        Credential w = new Credential();
        w.readFields(in);
        return w;
    }

    private byte nextTag(DataInput in) throws IOException {
        try {
            return in.readByte();
        }
        catch(EOFException ex) {
            return 0;
        }
    }

    public String serialize() {
        return Utils.serialize(this);
    }

    public static Credential deserialize(String in) {
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
