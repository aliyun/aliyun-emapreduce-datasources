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
package com.alibaba.dts.recordprocessor.mysql;

import java.util.HashMap;
import java.util.Map;

public class JDKEncodingMapper {
    private static final Map<String, String> MYSQL_JDK_ENCODINGS = new HashMap<String, String>();
    static{
        MYSQL_JDK_ENCODINGS.put("armscii8", "WINDOWS-1252");
        MYSQL_JDK_ENCODINGS.put("ascii", "US-ASCII");
        MYSQL_JDK_ENCODINGS.put("big5", "BIG5");
        MYSQL_JDK_ENCODINGS.put("binary", "ISO-8859-1");
        MYSQL_JDK_ENCODINGS.put("cp1250", "WINDOWS-1250");
        MYSQL_JDK_ENCODINGS.put("cp1251", "WINDOWS-1251");
        MYSQL_JDK_ENCODINGS.put("cp1256", "WINDOWS-1256");
        MYSQL_JDK_ENCODINGS.put("cp1257", "WINDOWS-1257");
        MYSQL_JDK_ENCODINGS.put("cp850", "IBM850");
        MYSQL_JDK_ENCODINGS.put("cp852", "IBM852");
        MYSQL_JDK_ENCODINGS.put("cp866", "IBM866");
        MYSQL_JDK_ENCODINGS.put("cp932", "WINDOWS-31J");
        MYSQL_JDK_ENCODINGS.put("dec8", "WINDOWS-1252");
        MYSQL_JDK_ENCODINGS.put("eucjpms", "X-EUCJP-OPEN");
        MYSQL_JDK_ENCODINGS.put("euckr", "EUC-KR");
        MYSQL_JDK_ENCODINGS.put("gb2312", "GB2312");
        MYSQL_JDK_ENCODINGS.put("gbk", "GBK");
        MYSQL_JDK_ENCODINGS.put("geostd8", "WINDOWS-1252");
        MYSQL_JDK_ENCODINGS.put("greek", "ISO-8859-7");
        MYSQL_JDK_ENCODINGS.put("hebrew", "ISO-8859-8");
        MYSQL_JDK_ENCODINGS.put("hp8", "WINDOWS-1252");
        MYSQL_JDK_ENCODINGS.put("keybcs2", "IBM852");
        MYSQL_JDK_ENCODINGS.put("koi8r", "KOI8-R");
        MYSQL_JDK_ENCODINGS.put("koi8u", "KOI8-R");
        MYSQL_JDK_ENCODINGS.put("latin1", "WINDOWS-1252");
        MYSQL_JDK_ENCODINGS.put("latin2", "ISO-8859-2");
        MYSQL_JDK_ENCODINGS.put("latin5", "ISO-8859-9");
        MYSQL_JDK_ENCODINGS.put("latin7", "ISO-8859-13");
        MYSQL_JDK_ENCODINGS.put("macce", "X-MACCENTRALEUROPE");
        MYSQL_JDK_ENCODINGS.put("macroman", "X-MACROMAN");
        MYSQL_JDK_ENCODINGS.put("sjis", "SHIFT_JIS");
        MYSQL_JDK_ENCODINGS.put("swe7", "WINDOWS-1252");
        MYSQL_JDK_ENCODINGS.put("tis620", "TIS-620");
        MYSQL_JDK_ENCODINGS.put("ujis", "EUC-JP");
        MYSQL_JDK_ENCODINGS.put("utf16", "UTF-16");
        MYSQL_JDK_ENCODINGS.put("utf16le", "UTF-16LE");
        MYSQL_JDK_ENCODINGS.put("utf32", "UTF-32");
        MYSQL_JDK_ENCODINGS.put("utf8", "UTF-8");
        MYSQL_JDK_ENCODINGS.put("utf8mb4", "UTF-8");
        MYSQL_JDK_ENCODINGS.put("ucs2", "UTF-16");
    }
    public static String getJDKEncoding(String  mysqlEncoding) {
        return MYSQL_JDK_ENCODINGS.get(mysqlEncoding);
    }
}
