/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.streaming.dsl.task

import java.time.Instant

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.dsl.plan.functions.DummyRunner
import org.apache.gearpump.streaming.dsl.window.api.GlobalWindows
import org.apache.gearpump.streaming.{Constants, MockUtil}
import org.apache.gearpump.streaming.dsl.window.impl.WindowOperator
import org.apache.gearpump.streaming.source.Watermark
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks

class GroupByTaskSpec extends PropSpec with PropertyChecks
  with Matchers with MockitoSugar {

  property("GroupByTask should trigger on watermark") {
    val longGen = Gen.chooseNum[Long](1L, 1000L).map(Instant.ofEpochMilli)

    forAll(longGen) { (time: Instant) =>
      val groupBy = mock[Any => Int]
      val windowRunner = new WindowOperator[Any, Any](GlobalWindows(), new DummyRunner[Any])
      val context = MockUtil.mockTaskContext
      val config = UserConfig.empty
        .withValue(
          Constants.GEARPUMP_STREAMING_OPERATOR, windowRunner)(MockUtil.system)

      val task = new GroupByTask[Any, Int, Any](groupBy, context, config)
      val value = time
      val message = Message(value, time)
      when(groupBy(time)).thenReturn(0)
      task.onNext(message)

      task.onWatermarkProgress(Watermark.MAX)
      verify(context).output(message)
    }
  }

}
