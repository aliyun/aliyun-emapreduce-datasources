/*
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
package com.alibaba.dts.recordprocessor;

import org.apache.commons.lang3.StringUtils;
import com.alibaba.dts.recordprocessor.mysql.JDKEncodingMapper;

import java.io.UnsupportedEncodingException;

public class FieldValue  {
    private String encoding;
    private byte[] bytes;
    public String getEncoding() {
        return encoding;
    }
    public byte[] getValue() {
        return bytes;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }
    public void setValue(byte[] bytes) {
        this.bytes = bytes;
    }
    @Override
    public String toString() {
        if (null == getValue()) {
            return "null [binary]";
        }
        if (encoding==null) {
            return super.toString();
        }
        try {
            if(StringUtils.equals("utf8mb4", encoding)){
                return new String(getValue(), "utf8");
            }else{
                return new String(getValue(), encoding);
            }
        } catch (UnsupportedEncodingException e) {
            String  realEncoding = JDKEncodingMapper.getJDKEncoding(encoding);
            if (null == realEncoding) {
                throw new RuntimeException("Unsupported encoding: " + encoding);
            } else {
                try {
                    return new String(getValue(), realEncoding);
                } catch (UnsupportedEncodingException e1) {
                    throw new RuntimeException("Unsupported encoding: origin "
                        +  encoding + ", mapped " + realEncoding);
                }
            }
        }
    }
}
