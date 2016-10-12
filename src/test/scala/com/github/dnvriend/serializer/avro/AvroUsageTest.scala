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
import com.github.dnvriend.avro.AvroFooBar
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericDatumWriter, GenericRecord }
import org.apache.avro.io.{ BinaryEncoder, Decoder, DecoderFactory, EncoderFactory }
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter }

class AvroUsageTest extends TestSpec {

  final val AvroFooBarV1 = "ACJ0aGlzIGlzIGEgdGVzdCB2MQ==" // optional field str
  final val AvroFooBarV2 = "ACJ0aGlzIGlzIGEgdGVzdCB2MgAC" // extra optional field x: Int
  final val AvroFooBarV3 = "ACJ0aGlzIGlzIGEgdGVzdCB2MwACAAQ=" // extra optional field y: Int
  final val AvroFooBarV4 = "AAIABA==" // removed field str

  final val AvroFooBarSchemaV1 =
    """
      |{
      | "type":"record",
      | "name":"AvroFooBar",
      | "namespace":"com.github.dnvriend.avro",
      | "fields":[
      |    {"name":"str","type":[{"type":"string","avro.java.string":"String"},"null"]}
      | ]
      |}
    """.stripMargin

  final val AvroFooBarSchemaV2 =
    """
      |{
      | "type":"record",
      | "name":"AvroFooBar",
      | "namespace":"com.github.dnvriend.avro",
      | "fields":[
      |     {"name":"str","type":[{"type":"string","avro.java.string":"String"},"null"]},
      |     {"name":"x", "type": ["int", "null"]}
      | ]
      |}
    """.stripMargin

  final val AvroFooBarSchemaV3 =
    """
      |{
      | "type":"record",
      | "name":"AvroFooBar",
      | "namespace":"com.github.dnvriend.avro",
      | "fields":[
      |     {"name":"str","type":[{"type":"string","avro.java.string":"String"},"null"]},
      |     {"name":"x", "type": ["int", "null"]},
      |     {"name":"y", "type": ["int", "null"]}
      | ]
      |}
    """.stripMargin

  final val AvroFooBarSchemaV4 =
    """
      |{
      | "type":"record",
      | "name":"AvroFooBar",
      | "namespace":"com.github.dnvriend.avro",
      | "fields":[
      |     {"name":"x", "type": ["int", "null"]},
      |     {"name":"y", "type": ["int", "null"]}
      | ]
      |}
    """.stripMargin

  it should "get the schema from fqcn" in {
    val clazz: Class[_] = Class.forName("com.github.dnvriend.avro.AvroFooBar")
    val method = clazz.getMethod("getClassSchema")
    val result = method.invoke(null)
    result shouldBe a[Schema]
  }

  it should "serialize using a generic datum writer" in {
    val foobar = new AvroFooBar("this is a test")
    val schema: Schema = foobar.getSchema
    val baos = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(baos, null)
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    datumWriter.write(foobar, encoder)
    encoder.flush()
    baos.close()
    val arr = baos.toByteArray
    arr.size shouldBe 16
  }

  it should "serialize using a specific datum writer" in {
    val foobar = new AvroFooBar("this is a test")
    val schema = foobar.getSchema
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    val writer = new SpecificDatumWriter[GenericRecord](schema)
    writer.write(foobar, encoder)
    encoder.flush()
    out.close()
    out.size() shouldBe 16
  }

  it should "deserialize using specific datum reader" in {
    // first serialize
    val foobar = new AvroFooBar("this is a test")
    val schema = foobar.getSchema
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    val writer = new SpecificDatumWriter[GenericRecord](schema)
    writer.write(foobar, encoder)
    encoder.flush()
    out.close()
    val bytes: Array[Byte] = out.toByteArray
    bytes.size shouldBe 16

    val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val reader = new SpecificDatumReader[AnyRef](AvroFooBar.getClassSchema)
    val objectRead = reader.read(null, decoder)
    objectRead.asInstanceOf[AvroFooBar].getStr shouldBe "this is a test"
  }

  def withReader(schema: String = AvroFooBarSchemaV1, data: String = AvroFooBarV1)(f: AvroFooBar => Unit) = {
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(data.toByteArray, null)
    val reader = new SpecificDatumReader[AnyRef](schema.toSchema)
    val objectRead = reader.read(null, decoder)
    f(objectRead.asInstanceOf[AvroFooBar])
  }

  it should "deserialize v1 with v1 schema" in withReader() { v1 =>
    v1.getStr shouldBe "this is a test v1"
  }

  it should "deserialize v2 with v1 schema" in withReader(data = AvroFooBarV2) { v2 =>
    v2.getStr shouldBe "this is a test v2"
  }

  it should "deserialize v3 with v1 schema" in withReader(data = AvroFooBarV3) { v3 =>
    v3.getStr shouldBe "this is a test v3"
  }

  it should "deserialize v4 with v1 schema" in withReader(data = AvroFooBarV4) { v4 =>
    intercept[NullPointerException] {
      // fields are available in the record, but schema doesn't know about it
      v4.asInstanceOf[GenericRecord].get("x")
      v4.asInstanceOf[GenericRecord].get("y")
    }
  }
}
