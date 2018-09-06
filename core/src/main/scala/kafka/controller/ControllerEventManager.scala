/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._

/**
  * 主要处理Controller和Broker、Topic、Partition之间的事件,主要包含：
  * case object ShutdownEventThread extends ControllerEvent
  * case object AutoPreferredReplicaLeaderElection extends ControllerEvent
  * case object UncleanLeaderElectionEnable extends ControllerEvent
  * case class ControlledShutdown(id: Int, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit) extends ControllerEvent
  * case class LeaderAndIsrResponseReceived(LeaderAndIsrResponseObj: AbstractResponse, brokerId: Int) extends ControllerEvent
  * case class TopicDeletionStopReplicaResponseReceived(stopReplicaResponseObj: AbstractResponse, replicaId: Int) extends ControllerEvent
  * case object Startup extends ControllerEvent 启动后选举事件
  * case object BrokerChange extends ControllerEvent
  * case class BrokerModifications(brokerId: Int) extends ControllerEvent
  * case object TopicChange extends ControllerEvent
  * case object LogDirEventNotification extends ControllerEvent
  * case class PartitionModifications(topic: String) extends ControllerEvent
  * case object TopicDeletion extends ControllerEvent
  * case object PartitionReassignment extends ControllerEvent
  * case class PartitionReassignmentIsrChange(partition: TopicPartition) extends ControllerEvent
  * case object IsrChangeNotification extends ControllerEvent
  * case object PreferredReplicaLeaderElection extends ControllerEvent
  * case object ControllerChange extends ControllerEvent
  * case object Reelect extends ControllerEvent
  * case object RegisterBrokerAndReelect extends ControllerEvent
  * class Expire extends ControllerEvent
  */
object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
}
class ControllerEventManager(controllerId: Int, rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
                             eventProcessedListener: ControllerEvent => Unit) extends KafkaMetricsGroup {

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  private val queue = new LinkedBlockingQueue[ControllerEvent]
  private val thread = new ControllerEventThread(ControllerEventManager.ControllerEventThreadName)
  private val time = Time.SYSTEM

  private val eventQueueTimeHist = newHistogram("EventQueueTimeMs")

  newGauge(
    "EventQueueSize",
    new Gauge[Int] {
      def value: Int = {
        queue.size()
      }
    }
  )


  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    clearAndPut(KafkaController.ShutdownEventThread)
    thread.awaitShutdown()
  }

  def put(event: ControllerEvent): Unit = inLock(putLock) {
    queue.put(event)
  }

  def clearAndPut(event: ControllerEvent): Unit = inLock(putLock) {
    queue.clear()
    put(event)
  }

  /**
    * ShutdownableThread方法是一个线程模板类，子类实现方法doWork后会被线程调用
    */
  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    override def doWork(): Unit = {
      queue.take() match {
        case KafkaController.ShutdownEventThread => initiateShutdown()
        //任务类（是ControllerEvent的子类）只要不是ShutdownEventThread 类，都走下面的case
        case controllerEvent =>
          _state = controllerEvent.state

          eventQueueTimeHist.update(time.milliseconds() - controllerEvent.enqueueTimeMs)

          try {
            //time()方法最终会启动一个RequestSendThread的线程，此类位于ControllerChannelManager中
            rateAndTimeMetrics(state).time {
              controllerEvent.process()
            }
          } catch {
            case e: Throwable => error(s"Error processing event $controllerEvent", e)
          }

          //此参数为一个高级函数，现在传入了一个参数
          try eventProcessedListener(controllerEvent)
          catch {
            case e: Throwable => error(s"Error while invoking listener for processed event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

}
