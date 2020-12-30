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

import com.alibaba.dts.formats.avro.Field;
import org.apache.commons.lang3.StringUtils;
import com.alibaba.dts.recordprocessor.mysql.MysqlFieldConverter;

public interface FieldConverter {
    FieldValue convert(Field field, Object o);
    static FieldConverter getConverter(String sourceName, String sourceVersion) {
        if (StringUtils.endsWithIgnoreCase("mysql", sourceName)) {
            return new MysqlFieldConverter();
        } else {
            throw new RuntimeException("FieldConverter: only mysql supported for now");
        }
    }
}
