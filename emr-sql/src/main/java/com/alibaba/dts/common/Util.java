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

package com.alibaba.dts.common;

import com.alibaba.dts.formats.avro.Record;

import java.util.List;

public class Util {
    public  static String[] uncompressionObjectName(String compressionName){
        if(null == compressionName || compressionName.isEmpty() ){
            return  null;
        }

        String [] names = compressionName.split("\\.");

        int length = names.length;

        for(int i=0;i<length;++i){
            names[i] = unescapeName(names[i]);
        }
        return names;
    }

    private  static String unescapeName(String name){
        if (null == name || (name.indexOf("\\u002E") < 0)) {
            return name;
        }

        StringBuilder builder = new StringBuilder();

        int length = name.length();

        for(int i=0;i<length;++i){
            char c = name.charAt(i);
            if('\\' == c && ( i<length-6 && 'u' == name.charAt(i + 1)
                    && '0' == name.charAt(i + 2) && '0' == name.charAt(i+3)
                    && '2' == name.charAt(i + 4) && 'E' == name.charAt(i+5))){
                builder.append(".");
                i += 5;
                continue;
            }else{
                builder.append(c);
            }
        }

        return builder.toString();
    }


    public static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception e) {
        }
    }

    public static FieldEntryHolder[] getFieldEntryHolder(Record record) {
        // this is a simple impl, may exist unhandled situation
        FieldEntryHolder[] fieldArray = new FieldEntryHolder[2];

        fieldArray[0] = new FieldEntryHolder((List<Object>) record.getBeforeImages());
        fieldArray[1] = new FieldEntryHolder((List<Object>) record.getAfterImages());

        return fieldArray;
    }
}
