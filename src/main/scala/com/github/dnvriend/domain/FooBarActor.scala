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

package com.github.dnvriend.domain

import akka.event.LoggingReceive
import akka.persistence.PersistentActor
import com.github.dnvriend.domain.FooBarActor.{ BazQux, FooBar }

object FooBarActor {
  sealed trait FooBarEvent
  final case class FooBar(str: String) extends FooBarEvent
  final case class BazQux(str: String) extends FooBarEvent
}

class FooBarActor(val persistenceId: String) extends PersistentActor {
  override def receiveRecover: Receive = LoggingReceive {
    case e => println("Recovering with: fqcn: " + e.getClass.getName + ", event: " + e)
  }

  override def receiveCommand: Receive = LoggingReceive {
    case "foo"       => persist(FooBar("foo"))(_ => sender() ! akka.actor.Status.Success())
    case str: String => persist(BazQux(str))(_ => sender() ! akka.actor.Status.Success())
  }

  override protected def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
    println("onPersistFailure: " + cause.getMessage)
    super.onPersistFailure(cause, event, seqNr)
  }

  override protected def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
    println("onPersistRejected: " + cause.getMessage)
    super.onPersistRejected(cause, event, seqNr)
  }
}
