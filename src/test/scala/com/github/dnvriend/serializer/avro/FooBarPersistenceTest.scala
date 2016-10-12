/*
 * Copyright 2016 Dennis Vriend
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dnvriend.serializer.avro

import akka.actor.{ ActorRef, Props }
import akka.testkit.TestProbe
import com.github.dnvriend.TestSpec
import com.github.dnvriend.domain.FooBarActor
import com.github.dnvriend.domain.FooBarActor.{ BazQux, FooBar }

class FooBarPersistenceTest extends TestSpec {

  it should "fooBar" in {
    withActor() { tp => ref =>
      tp.send(ref, "foo")
      tp.expectMsg(akka.actor.Status.Success())
    }

    withActor() { tp => ref =>
      tp.send(ref, "bar")
      tp.expectMsg(akka.actor.Status.Success())
    }

    println("============================")
    println("=== READING FROM JOURNAL ===")
    println("============================")

    // akka-persistence-inmemory uses the write-journal's event adapters
    eventsForPersistenceIdSource("p1")
      .map(_.event)
      .testProbe { tp ⇒
        tp.request(Int.MaxValue)
        tp.expectNext(FooBar("foo"))
        tp.expectNext(BazQux("bar"))
        tp.expectComplete()
      }
  }

  def withActor(id: String = "p1")(f: TestProbe => ActorRef => Unit): Unit = {
    val tp = TestProbe()
    val ref = system.actorOf(Props(new FooBarActor(id)))
    try f(tp)(ref) finally killActors(ref)
  }

}
