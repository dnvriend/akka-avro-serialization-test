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

package com.github.dnvriend.adapter

import akka.persistence.journal.{ EventSeq, ReadEventAdapter }
import com.github.dnvriend.avro.{ AvroBazQux, AvroFooBar }
import com.github.dnvriend.domain.FooBarActor.{ BazQux, FooBar }

class AvroReadEventAdapter extends ReadEventAdapter {
  override def fromJournal(o: Any, manifest: String): EventSeq = {
    println(s"AvroReadEventAdapter.fromJournal with manifest (type-hint): '$manifest'")
    o match {
      case event: AvroFooBar => EventSeq(FooBar(event.getStr))
      case event: AvroBazQux => EventSeq(BazQux(event.getStr))
    }
  }
}
