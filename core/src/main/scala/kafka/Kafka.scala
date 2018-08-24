/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka

import java.util.Properties

import joptsimple.OptionParser
import kafka.utils.Implicits._
import kafka.server.{KafkaServer, KafkaServerStartable}
import kafka.utils.{CommandLineUtils, Exit, Logging}
import org.apache.kafka.common.utils.{Java, LoggingSignalHandler, OperatingSystem, Utils}
import org.apache.log4j.PropertyConfigurator

import scala.collection.JavaConverters._

/**
  * 1. 配置程序参数为：config\server.properties
  * 2. 拷贝log4j.properties文件到源码跟路径下
  */
object Kafka extends Logging {

  def getPropsFromArgs(args: Array[String]): Properties = {
    val optionParser = new OptionParser(false)
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])

    if (args.length == 0) {
      CommandLineUtils.printUsageAndDie(optionParser, "USAGE: java [options] %s server.properties [--override property=value]*".format(classOf[KafkaServer].getSimpleName()))
    }

    val props = Utils.loadProps(args(0))

    if (args.length > 1) {
      //_*操作符会转换成序列参数(变长参数)，参考《快学scala》2.1节
      //解析选项:kafka.kafka config\server.properties --override num.partitions=2
      val options = optionParser.parse(args.slice(1, args.length): _*)

      if (options.nonOptionArguments().size() > 0) {
        CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
      }

      //++连接两个集合成一个，asScala把java集合转换成scala集合
      props ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt).asScala)
    }
    props
  }

  def main(args: Array[String]): Unit = {
    //PropertyConfigurator.configure("config\\log4j.        properties")

    try {
      val serverProps = getPropsFromArgs(args)
      val kafkaServerStartable = KafkaServerStartable.fromProps(serverProps)

      try {
        if (!OperatingSystem.IS_WINDOWS && !Java.isIbmJdk)
          new LoggingSignalHandler().register()
      } catch {
        case e: ReflectiveOperationException =>
          warn("Failed to register optional signal handler that logs a message when the process is terminated " +
            s"by a signal. Reason for registration failure is: $e", e)
      }

      // attach shutdown handler to catch terminating signals as well as normal termination
      Runtime.getRuntime().addShutdownHook(new Thread("kafka-shutdown-hook") {
        override def run(): Unit = kafkaServerStartable.shutdown()
      })

      kafkaServerStartable.startup()
      kafkaServerStartable.awaitShutdown()
    }
    catch {
      case e: Throwable =>
        fatal("Exiting Kafka due to fatal exception", e)
        Exit.exit(1)
    }
    Exit.exit(0)
  }
}
