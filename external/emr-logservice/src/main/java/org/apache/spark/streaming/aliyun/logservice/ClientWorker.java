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
package org.apache.spark.streaming.aliyun.logservice;

import com.aliyun.ms.MetaClient;
import com.aliyun.ms.utils.EndpointEnum;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.LogHubClientAdapter;
import com.aliyun.openservices.loghub.client.LogHubConsumer;
import com.aliyun.openservices.loghub.client.LogHubHeartBeat;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ClientWorker implements Runnable {
  private final ILogHubProcessorFactory mLogHubProcessorFactory;
  private final LogHubConfig mLogHubConfig;
  private final LogHubHeartBeat mLogHubHeartBeat;
  private boolean mShutDown = false;
  private final Map<Integer, LogHubConsumer> mShardConsumer =
      new HashMap<Integer, LogHubConsumer>();
  private final ExecutorService mExecutorService =
      Executors.newCachedThreadPool();
  private LogHubClientAdapter mLogHubClientAdapter;
  private Client mClient;
  private String loghubEndpoint;
  private static final Log LOG = LogFactory.getLog(ClientWorker.class);

  public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config)
      throws LogHubClientWorkerException {
    this.mLogHubProcessorFactory = factory;
    this.mLogHubConfig = config;

    String accessKeyId = config.getAccessId();
    String accessKeySecret = config.getAccessKey();
    String securityToken = null;
    this.loghubEndpoint = config.getLogHubEndPoint();
    if (loghubEndpoint == null) {
      String region = MetaClient.getClusterRegionName();
      String nType = MetaClient.getClusterNetworkType();
      String project = config.getProject();
      loghubEndpoint = project + "." +
          EndpointEnum.getEndpoint("log", region, nType);
    }

    // Check accessKeyId and accessKeySecret are null ot not.
    if (accessKeyId == null || accessKeySecret == null) {
      accessKeyId = MetaClient.getRoleAccessKeyId();
      accessKeySecret = MetaClient.getRoleAccessKeySecret();
      securityToken = MetaClient.getRoleSecurityToken();

      this.mClient = new Client(loghubEndpoint, accessKeyId, accessKeySecret);
      this.mClient.SetSecurityToken(securityToken);
    } else {
      this.mClient = new Client(loghubEndpoint, accessKeyId, accessKeySecret);
    }

    this.mLogHubClientAdapter = new LogHubClientAdapter(
        loghubEndpoint, accessKeyId, accessKeySecret,
        securityToken, config.getProject(), config.getLogStore(),
        config.getConsumerGroupName(), config.getWorkerInstanceName());

    try {
      this.mLogHubClientAdapter.CreateConsumerGroup(
          (int)(config.getHeartBeatIntervalMillis() * 2L / 1000L),
          config.isConsumeInOrder());
    } catch (LogException e) {
      label: {
        if(e.GetErrorCode()
            .compareToIgnoreCase("ConsumerGroupAlreadyExist") == 0) {
          try {
            ConsumerGroup cg = this.mLogHubClientAdapter.GetConsumerGroup();
            if(cg == null) {
              throw new LogHubClientWorkerException("consumer group not exist");
            }

            if(cg.isInOrder() == this.mLogHubConfig.isConsumeInOrder()
                && cg.getTimeout() == (int)(this.mLogHubConfig
                    .getHeartBeatIntervalMillis() * 2L / 1000L)) {
              break label;
            }

            throw new LogHubClientWorkerException("consumer group is not " +
                "agreed, AlreadyExistedConsumerGroup: {\"consumeInOrder\": " +
                cg.isInOrder() + ", \"timeoutInMillSecond\": " +
                cg.getTimeout() + "}");
          } catch (LogException e1) {
            throw new LogHubClientWorkerException("error occur when get " +
                "consumer group, errorCode: " + e1.GetErrorCode() +
                ", errorMessage: " + e1.GetErrorMessage());
          }
        }

        throw new LogHubClientWorkerException("error occur when create " +
            "consumer group, errorCode: " + e.GetErrorCode() +
            ", errorMessage: " + e.GetErrorMessage());
      }
    }

    this.mLogHubHeartBeat = new LogHubHeartBeat(this.mLogHubClientAdapter,
        config.getHeartBeatIntervalMillis());
  }

  private void switchClient(String accessKeyId, String accessKey, String stsToken) {
    this.mLogHubClientAdapter
        .SwitchClient(this.loghubEndpoint, accessKeyId, accessKey, stsToken);
  }

  public void run() {
    this.mLogHubHeartBeat.Start();
    ArrayList<Integer> heldShards = new ArrayList<Integer>();

    while(!this.mShutDown) {
      this.mLogHubHeartBeat.GetHeldShards(heldShards);
      Iterator var2 = heldShards.iterator();

      while(var2.hasNext()) {
        int shard = (Integer) var2.next();
        LogHubConsumer consumer = this.getConsumer(shard);
        consumer.consume();
      }

      this.cleanConsumer(heldShards);

      try {
        Thread.sleep(this.mLogHubConfig.getDataFetchIntervalMillis());
        checkAndUpdateToken();
      } catch (InterruptedException e) {
        // make compiler happy.
      }
    }

  }

  public void shutdown() {
    this.mShutDown = true;
    this.mLogHubHeartBeat.Stop();
  }

  private void cleanConsumer(ArrayList<Integer> ownedShard) {
    ArrayList<Integer> removeShards = new ArrayList<Integer>();
    Iterator it = this.mShardConsumer.entrySet().iterator();

    while(it.hasNext()) {
      Entry shard = (Entry) it.next();
      LogHubConsumer consumer = (LogHubConsumer)shard.getValue();
      if(!ownedShard.contains((Integer) shard.getKey())) {
        consumer.shutdown();
        LOG.warn("try to shut down a consumer shard:" + shard.getKey());
      }

      if(consumer.isShutdown()) {
        this.mLogHubHeartBeat.RemoveHeartShard((Integer) shard.getKey());
        removeShards.add((Integer) shard.getKey());
        LOG.warn("remove a consumer shard:" + shard.getKey());
      }
    }

    it = removeShards.iterator();

    while(it.hasNext()) {
      int shard1 = (Integer) it.next();
      this.mShardConsumer.remove(shard1);
    }

  }

  private LogHubConsumer getConsumer(int shardId) {
    LogHubConsumer consumer = this.mShardConsumer.get(shardId);
    if(consumer != null) {
      return consumer;
    } else {
      consumer = new LogHubConsumer(
          this.mLogHubClientAdapter,
          shardId,
          this.mLogHubConfig.getWorkerInstanceName(),
          this.mLogHubProcessorFactory.generatorProcessor(),
          this.mExecutorService,
          this.mLogHubConfig.getCursorPosition(),
          this.mLogHubConfig.GetCursorStartTime());
      this.mShardConsumer.put(shardId, consumer);
      LOG.warn("create a consumer shard:" + shardId);
      return consumer;
    }
  }

  private void checkAndUpdateToken() {
    try {
      this.mClient.HeartBeat(mLogHubConfig.getProject(),
          mLogHubConfig.getLogStore(), mLogHubConfig.getConsumerGroupName(),
          mLogHubConfig.getWorkerInstanceName(), new ArrayList<Integer>());
    } catch (LogException e) {
      String accessKeyId = MetaClient.getRoleAccessKeyId();
      String accessKeySecret = MetaClient.getRoleAccessKeySecret();
      String securityToken = MetaClient.getRoleSecurityToken();

      this.mClient = new Client(loghubEndpoint, accessKeyId, accessKeySecret);
      this.mClient.SetSecurityToken(securityToken);
      switchClient(accessKeyId, accessKeySecret, securityToken);
    }
  }
}
