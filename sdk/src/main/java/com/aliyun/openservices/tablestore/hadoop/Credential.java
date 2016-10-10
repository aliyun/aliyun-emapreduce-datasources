package com.aliyun.openservices.tablestore.hadoop;

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
    public static final String kTableStoreCredential = "TABLESTORE_CREDENTIAL";
    
    public String akId;
    public String akSecret;
    public String stsToken;

    public Credential() {
    }
    public Credential(String akId, String akSecret, String stsToken) {
        Preconditions.checkNotNull(akId, "akId should not be null.");
        Preconditions.checkNotNull(akSecret, "akSecret should not be null.");
        this.akId = akId;
        this.akSecret = akSecret;
        this.stsToken = stsToken;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Credential)) {
            return false;
        }
        Credential a = (Credential) obj;
        if (!this.akId.equals(a.akId)) {
            return false;
        }
        if (!this.akSecret.equals(a.akSecret)) {
            return false;
        }
        if (this.stsToken == null) {
            return a.stsToken == null;
        } else {
            return this.stsToken.equals(a.stsToken);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(WritableConsts.CREDENTIAL);
        out.writeUTF(akId);
        out.writeUTF(akSecret);
        if (stsToken != null) {
            out.writeByte(WritableConsts.CREDENTIAL_STS_TOKEN);
            out.writeUTF(stsToken);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag != WritableConsts.CREDENTIAL) {
            throw new IOException("broken input stream");
        }
        akId = null;
        akSecret = null;
        stsToken = null;
        akId = in.readUTF();
        akSecret = in.readUTF();
        byte tagSts = nextTag(in);
        if (tagSts == WritableConsts.CREDENTIAL_STS_TOKEN) {
            stsToken = in.readUTF();
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
