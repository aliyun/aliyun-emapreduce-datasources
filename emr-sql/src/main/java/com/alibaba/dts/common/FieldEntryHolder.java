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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class FieldEntryHolder {
    private final List<Object> originFields;
    private final Iterator<Object> iterator;
    private final List<Object> filteredFields;
    public FieldEntryHolder(List<Object> originFields) {
        this.originFields = originFields;
        if (null == originFields) {
            this.filteredFields = null;
            this.iterator = null;
        } else {
            this.filteredFields = new LinkedList<>();
            this.iterator = originFields.iterator();
        }
    }

    public boolean hasNext() {
        if (iterator == null) {
            return true;
        }
        return iterator.hasNext();
    }

    public void skip() {
        if (null != iterator) {
            iterator.next();
        }
    }

    public Object take() {
        if (null != iterator) {
            Object current = iterator.next();
            filteredFields.add(current);
            return  current;
        } else {
            return null;
        }
    }
}
