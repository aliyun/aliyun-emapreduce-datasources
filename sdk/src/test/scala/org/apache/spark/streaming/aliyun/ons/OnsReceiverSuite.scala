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
package org.apache.spark.streaming.aliyun.ons

import com.aliyun.openservices.ons.api.Message
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Duration, Seconds}
import org.apache.spark.util.Clock
import org.mockito.Mockito._
import org.mockito.Mockito._
import org.scalatest.{FunSuite, Matchers, BeforeAndAfter}
import org.scalatest.mock.MockitoSugar

class OnsReceiverSuite extends FunSuite with BeforeAndAfter with Matchers with MockitoSugar {

  val master = "local[2]"
  val appName = this.getClass.getSimpleName
  val batchDuration: Duration = Seconds(1)

  val record1 = new Message()
  record1.setBody("Spark In Action".getBytes())
  val record2 = new Message()
  record2.setBody("Learning Spark".getBytes())
  val batch = List[Message](record1, record2)

  var receiverMock: OnsReceiver = _
  var currentClockMock: Clock = _

  def func: Message => Array[Byte] = message => message.getBody

  private def beforeFunction(): Unit = {
    receiverMock = mock[OnsReceiver]
    currentClockMock = mock[Clock]
  }
  before(beforeFunction)

  private def afterFunction(): Unit = {
    verifyNoMoreInteractions(receiverMock, currentClockMock)
  }
  after(afterFunction)

  test("OnsUtils API") {
    val ssc = new StreamingContext(master, appName, batchDuration)

    val onsStream1 = OnsUtils.createStream(ssc, "CID-001", "topic", "tags", "accesskeyId", "accessKeySecret",
      StorageLevel.MEMORY_AND_DISK_2, func)
    val onsStream2 = OnsUtils.createStreams(ssc, Array(("CID-002", "topic", "tags")), "accessKeyId", "accessKeySecret",
      StorageLevel.MEMORY_AND_DISK_2, func)

    ssc.stop()
  }
}
