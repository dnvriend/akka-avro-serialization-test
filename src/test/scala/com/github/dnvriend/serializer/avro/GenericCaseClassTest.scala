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

import java.io.ByteArrayOutputStream

import com.github.dnvriend.TestSpec
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter

final case class AvroFoo(x: Int, y: Int, z: String) extends AvroMarker {
  override def avsc: String = """{"type":"record","name":"AvroFoo","namespace":"com.github.dnvriend.serializer.avro","fields":[{"name":"x","type":"int"},{"name":"y","type":"int"},{"name":"z","type":"string"}]}"""
  override def record: GenericRecord = {
    val record = new GenericData.Record(schema)
    record.put("x", x)
    record.put("y", y)
    record.put("z", z)
    record
  }
}

class GenericCaseClassTest extends TestSpec {
  def serialize(marker: AvroMarker): Array[Byte] = {
    println(marker)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    val writer = new SpecificDatumWriter[GenericRecord](marker.schema)
    writer.write(marker.record, encoder)
    encoder.flush()
    out.close()
    out.toByteArray
  }

  def deserialize(bytes: Array[Byte], manifest: String): AnyRef = {
    val clazz: Class[_] = Class.forName(manifest)
    clazz.getDeclaredMethods.toList.foreach(println)
    val method = clazz.getMethod("schema")

    //    val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    //    val reader = new SpecificDatumReader[AnyRef](schema)
    //    reader.read(null, decoder)
    ""
  }

  it should "serialize" in {
    val foo = AvroFoo(1, 2, "foo")
    val bytes = serialize(foo)
    deserialize(bytes, foo.getClass.getName)
    //    val genericRecord: GenericRecord = foo.toRecord
    //    foo.fromRecord(genericRecord) shouldBe AvroFoo(1, 2, "foo")

    //    serialize(foo)
  }
}
