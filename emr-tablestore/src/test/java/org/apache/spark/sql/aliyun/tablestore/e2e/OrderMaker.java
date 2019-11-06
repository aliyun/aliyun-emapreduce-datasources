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
package org.apache.spark.sql.aliyun.tablestore.e2e;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.google.common.util.concurrent.AtomicDouble;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

// Parallel write test Order data to TableStore and got the agg result by the window.
// The spark agg sample see at [[StructuredTableStoreAggSample.scala]]
// Schema: {"columns": {"UserId": {"col": "UserId", "type": "string"}, "OrderId": {"col": "OrderId", "type": "string"},
//          "price": {"cols": "price", "type": "long"}, "timestamp": {"cols": "timestamp", "type": "long"}}}
// PrimaryKey: UserId/string, OrderId/string

public class OrderMaker {
    private OrderConfig config = OrderConfig.load("config.json");

    private AtomicLong currCount = new AtomicLong(0);
    private AtomicDouble currTotalPrice = new AtomicDouble(0);
    private long windowMillis = 30 * 1000;
    private AtomicLong windowEnd = new AtomicLong(0);
    private final Object windowLock = new Object();
    private SyncClient syncClient;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public OrderMaker() {
        long time = (System.currentTimeMillis() / windowMillis) * windowMillis;
        windowEnd.set(time + windowMillis);
        syncClient = new SyncClient(config.getEndpoint(), config.getAccessId(), config.getAccessKey(), config.getInstanceName());
    }

    public void make(int parallel) {
        ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(parallel);
        for (int i = 0; i < parallel; i++) {
            scheduledExecutor.scheduleWithFixedDelay(new BatchWriter(i), 0, 10, TimeUnit.MILLISECONDS);
        }
    }

    private class BatchWriter implements Runnable {
        private int id;
        private int tmpPrice = 1;

        public BatchWriter(int id) {
            this.id = id;
        }

        public void run() {
            try {
                BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
                Random rand = new Random(System.currentTimeMillis());
                for (int i = 0; i < 200; i++) {
                    PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                    String letters = "ABCDEFGHIJKLMNOP";
                    String userId = "user_" + letters.charAt(rand.nextInt(letters.length()));
                    pkBuilder.addPrimaryKeyColumn("UserId", PrimaryKeyValue.fromString(userId));
                    String orderId = UUID.randomUUID().toString();
                    pkBuilder.addPrimaryKeyColumn("OrderId", PrimaryKeyValue.fromString(orderId));

                    RowPutChange rowPutChange = new RowPutChange(config.getSourceTable(), pkBuilder.build());
                    long now = System.currentTimeMillis();
                    if (now >= windowEnd.get()) {
                        synchronized (windowLock) {
                            if (now >= windowEnd.get()) {
                                String start = dateFormat.format(new Date(windowEnd.get() - windowMillis));
                                String end = dateFormat.format(new Date(windowEnd.get()));
                                System.out.println(String.format("%s - %s, count: %d, price: %.2f",
                                        start, end, currCount.get(), currTotalPrice.get()));
                                // TODO: Write row to ExpectOrder sink table.
                                currCount.set(0);
                                currTotalPrice.set(0);
                                windowEnd.addAndGet(windowMillis);
                                tmpPrice = (tmpPrice + 1) % 10;
                            }
                        }
                    }
                    rowPutChange.addColumn(new Column("timestamp", ColumnValue.fromLong(now)));
                    double price = rand.nextInt(10) + (double) Math.round(rand.nextFloat() * 100) / 100;
                    rowPutChange.addColumn(new Column("price", ColumnValue.fromDouble(price)));
                    batchWriteRowRequest.addRowChange(rowPutChange);
                    currCount.incrementAndGet();
                    currTotalPrice.addAndGet(price);
                }
                syncClient.batchWriteRow(batchWriteRowRequest);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        OrderMaker orderMaker = new OrderMaker();
        orderMaker.make(1);
    }
}
