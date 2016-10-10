package com.aliyun.openservices.tablestore.hadoop;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;

class Utils {
    static String serialize(Writable w) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(os);
        try {
            w.write(out);
            out.close();
        } catch(IOException ex) {
            // intend to ignore
        }
        byte[] buf = os.toByteArray();
        return Base64.encodeBase64String(buf);
    }

}

