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
package org.apache.spark.streaming.aliyun.mns

import java.io.{InputStreamReader, BufferedReader, DataInputStream}
import java.net._
import java.security.cert.CertificateFactory
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Locale, Collections}
import javax.xml.parsers.DocumentBuilderFactory
import org.apache.commons.codec.binary.Base64
import org.apache.commons.httpclient.HttpStatus
import org.apache.http.{HttpEntityEnclosingRequest, MethodNotSupportedException, HttpResponse, HttpRequest}
import org.apache.http.protocol._
import org.apache.spark.Logging
import org.w3c.dom.Element

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HttpEndpoint(port: Int = 80) extends Logging {
  var thread: Thread = null

  private def genEndpointLocal(): String = {
    genEndpointLocal(80)
  }

  private def genEndpointLocal(port: Int) = {
    try {
      val addr = InetAddress.getLocalHost
      val ip = addr.getHostAddress
      s"http://$ip:$port"
    } catch {
      case ex: UnknownHostException =>
        log.warn("get local host fail.", ex)
        s"http://127.0.0.1:$port"
    }
  }

  def start(): Unit = {
    try {
      new Nothing(InetAddress.getLocalHost, this.port)
      log.warn(s"port $port already in use, http server start failed")
      throw new BindException(s"port $port already in use")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val httpProc = HttpProcessorBuilder.create
        .add(new ResponseDate())
        .add(new ResponseServer("MNS-Endpoint/1.1"))
        .add(new ResponseContent())
        .add(new ResponseConnControl).build()

    val registry = new UriHttpRequestHandlerMapper()
    registry.register("/notifications", new NSHandler)
    registry.register("/simplified", new SimplifiedNSHandler)

    val httpService = new HttpService(httpProc, registry)
    this.thread = new RequestListenerThread(port, httpService, null)
    this.thread.setDaemon(false)
    this.thread.start()
  }

  def stop(): Unit = {
    if (this.thread != null) {
      this.thread.interrupt()
      try {
        thread.join(10 * 1000)
      } catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
    }

    log.info("HttpEndpoint stop.")
  }

  private def authenticate(method: String, uri: String, headers: mutable.HashMap[String, String], cert: String): Boolean = {
    val str2sign = getSignString(method, uri, headers)
    val signature: String = headers.get("Authorization").get
    val decodedSign = Base64.decodeBase64(signature)

    try {
      //String cert = "http://mnstest.oss-cn-hangzhou.aliyuncs.com/x509_public_certificate.pem"
      val url = new URL(cert)
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      val in = new DataInputStream(conn.getInputStream)
      val cf = CertificateFactory.getInstance("X.509")
      val c = cf.generateCertificate(in)
      val pk = c.getPublicKey()
      val signetCheck = java.security.Signature.getInstance("SHA1withRSA")
      signetCheck.initVerify(pk)
      signetCheck.update(str2sign.getBytes())
      signetCheck.verify(decodedSign)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        log.warn(s"authenticate fail, ${e.getMessage}")
        false
    }
  }

  private def getSignString(method: String, uri: String, headers: mutable.HashMap[String, String]): String = {
    val sb = new StringBuilder()
    sb.append(method)
    sb.append("\n")
    sb.append(safeGetHeader(headers, "Content-md5"))
    sb.append("\n")
    sb.append(safeGetHeader(headers, "Content-Type"))
    sb.append("\n")
    sb.append(safeGetHeader(headers, "Date"))
    sb.append("\n")

    val tmp = new ArrayBuffer[String]()
    headers.filter(_._1.startsWith("x-mns-")).foreach(e => tmp.+=(s"${e._1}:${e._2}"))
    tmp.sorted.foreach(e => {
      sb.append(e)
      sb.append("\n")
    })

    sb.append(uri)
    sb.toString()
  }

  private def safeGetHeader(headers: mutable.HashMap[String, String], name: String): String = {
    if (headers.contains(name)) {
      headers.get(name).get
    } else {
      ""
    }
  }

  class SimplifiedNSHandler extends HttpRequestHandler {
    override def handle(httpRequest: HttpRequest, httpResponse: HttpResponse, httpContext: HttpContext): Unit = {
      val method = httpRequest.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH)

      if (!method.equals("GET") && !method.equals("HEAD") && !method.equals("POST")) {
        throw new MethodNotSupportedException(s"$method method not supported.")
      }

      val headers = httpRequest.getAllHeaders
      val hm = new mutable.HashMap[String, String]()
      headers.foreach(h =>hm.put(h.getName, h.getValue))

      val target = httpRequest.getRequestLine.getUri
      println(target)

      if (httpRequest.isInstanceOf[HttpEntityEnclosingRequest]) {
        val entity = httpRequest.asInstanceOf[HttpEntityEnclosingRequest].getEntity
        val certHeader = httpRequest.getFirstHeader("x-mns-signing-cert-url")
        if (certHeader == null) {
          log.warn("SigningCerURL Header not found")
          httpResponse.setStatusCode(HttpStatus.SC_BAD_REQUEST)
          return
        }

        var cert = certHeader.getValue
        if (cert.isEmpty) {
          log.warn("SigningCertURL empty")
          httpResponse.setStatusCode(HttpStatus.SC_BAD_REQUEST)
          return
        }

        cert = new String(Base64.decodeBase64(cert))
        log.debug(s"SigningCertURL: $cert")

        if (!authenticate(method, target, hm, cert)) {
          log.warn("authenticate fail")
          httpResponse.setStatusCode(HttpStatus.SC_BAD_REQUEST)
          return
        }

        val is = entity.getContent
        val reader = new BufferedReader(new InputStreamReader(is))
        val buffer = new StringBuffer()
        var line = reader.readLine
        while(line != null) {
          buffer.append(line)
          line = reader.readLine
        }

        val content = buffer.toString
        println(s"Simplified Notification: $content")
      }

      httpResponse.setStatusCode(HttpStatus.SC_NO_CONTENT)
    }
  }

  class NSHandler extends HttpRequestHandler {

    private def safeGetElementContent(element: Element, tag: String): String = {
      val nl = element.getElementsByTagName(tag)
      if (nl != null && nl.getLength > 0) {
        nl.item(0).getTextContent
      } else {
        log.warn(s"get $tag from xml fail")
        ""
      }
    }

    private def paserContent(notify: Element): Unit = {
      try {
        val topicOwner = safeGetElementContent(notify, "TopicOwner")
        log.debug(s"TopicOwner: $topicOwner")

        val topicName = safeGetElementContent(notify, "TopicName")
        log.debug(s"TopicName: $topicName")

        val subscriber = safeGetElementContent(notify, "Subscriber")
        log.debug(s"Subscriber: $subscriber")

        val subscriptionName = safeGetElementContent(notify, "SubscriptionName")
        log.debug(s"SubscriptionName: $subscriptionName")

        val msgid = safeGetElementContent(notify, "MessageId")
        log.debug(s"MessageId: $msgid")

        // if PublishMessage with base64 message
        val msg = safeGetElementContent(notify, "Message")
        log.debug(s"Message: ${new String(Base64.decodeBase64(msg))}")

        // if PublishMessage with string message
//        val msg = safeGetElementContent(notify, "Message")
//        log.debug(s"Message: $msg")

        val msgMD5 = safeGetElementContent(notify, "MessageMD5")
        log.debug(s"MessageMD5: $msgMD5")

        val msgPublishTime = safeGetElementContent(notify, "PublishTime")
        val date = new Date(msgPublishTime.toLong)
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dateString = sdf.format(date)
        log.debug(s"MessagePublishTime: $dateString")
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

    override def handle(httpRequest: HttpRequest, httpResponse: HttpResponse, httpContext: HttpContext): Unit = {
      val method = httpRequest.getRequestLine.getMethod.toUpperCase(Locale.ENGLISH)
      if (!method.equals("GET") && !method.equals("HEAD") && !method.equals("POST")) {
        throw new MethodNotSupportedException(s"$method method not supported")
      }

      val headers = httpRequest.getAllHeaders
      val hm = new mutable.HashMap[String, String]()
      headers.foreach(h => hm.put(h.getName, h.getValue))

      val target = httpRequest.getRequestLine.getUri
      if (httpRequest.isInstanceOf[HttpEntityEnclosingRequest]) {
        val entity =httpRequest.asInstanceOf[HttpEntityEnclosingRequest].getEntity

        val content = entity.getContent
        val dbf = DocumentBuilderFactory.newInstance
      }
    }
  }
}