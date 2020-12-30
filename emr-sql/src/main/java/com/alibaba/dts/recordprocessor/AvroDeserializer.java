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

import com.alibaba.dts.formats.avro.Record;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroDeserializer {
    private static final Logger log = LoggerFactory.getLogger(AvroDeserializer.class);

    private final SpecificDatumReader<Record> reader =
        new SpecificDatumReader<>(com.alibaba.dts.formats.avro.Record.class);

    public AvroDeserializer() {}

    public com.alibaba.dts.formats.avro.Record deserialize(byte[] data) {

        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        Record payload = null;
        try {
            payload = reader.read(null, decoder);
            return payload;
        }catch (Throwable ex) {
            log.error("AvroDeserializer: deserialize record failed cause " + ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }
}
