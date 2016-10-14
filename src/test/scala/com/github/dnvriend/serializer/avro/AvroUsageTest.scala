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
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{ GenericDatumReader, GenericDatumWriter, GenericRecord }
import org.apache.avro.io._
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter }

class AvroUsageTest extends TestSpec {

  def withAvroWriter(writerSchema: Schema)(f: Record => (GenericRecord => Unit) => Unit): Array[Byte] = {
    val record = new Record(writerSchema)
    val out = new ByteArrayOutputStream()
    val encoder: Encoder = EncoderFactory.get().binaryEncoder(out, null)
    val writer = new GenericDatumWriter[GenericRecord](writerSchema)
    f(record)(writer.write(_: GenericRecord, encoder))
    encoder.flush()
    out.close()
    out.toByteArray
  }

  def AvroFooBarV1: Array[Byte] = withAvroWriter(FooBarSchemaV1) { record => writer =>
    record.put("str", "this is a test v1")
    writer(record)
  }

  def AvroFooBarV2: Array[Byte] = withAvroWriter(FooBarSchemaV2) { record => writer =>
    record.put("str", "this is a test v2")
    record.put("x", 1)
    writer(record)
  }

  def AvroFooBarV3: Array[Byte] = withAvroWriter(FooBarSchemaV3) { record => writer =>
    record.put("str", "this is a test v3")
    record.put("x", 1)
    record.put("y", 2)
    writer(record)
  }

  def withJsonWriter(writerSchema: Schema)(f: Record => (GenericRecord => Unit) => Unit): String = {
    val record = new Record(writerSchema)
    val out = new ByteArrayOutputStream()
    val encoder: Encoder = EncoderFactory.get().jsonEncoder(writerSchema, out)
    val writer = new GenericDatumWriter[GenericRecord](writerSchema)
    f(record)(writer.write(_: GenericRecord, encoder))
    encoder.flush()
    out.close()
    new String(out.toByteArray)
  }

  // {"str":{"string":"this is a test v1 json"}}
  def JsonFooBarV1: String = withJsonWriter(FooBarSchemaV1) { record => writer =>
    record.put("str", "this is a test v1 json")
    writer(record)
  }

  // {"str":{"string":"this is a test v2 json"},"x":1}
  def JsonFooBarV2: String = withJsonWriter(FooBarSchemaV2) { record => writer =>
    record.put("str", "this is a test v2 json")
    record.put("x", 1)
    writer(record)
  }

  // {"str":{"string":"this is a test v3 json"},"x":1,"y":2}
  def JsonFooBarV3: String = withJsonWriter(FooBarSchemaV3) { record => writer =>
    record.put("str", "this is a test v3 json")
    record.put("x", 1)
    record.put("y", 2)
    writer(record)
  }

  final val FooBarSchemaV1 =
    """
      |{
      | "type":"record",
      | "name":"AvroFooBar",
      | "namespace":"com.github.dnvriend.avro",
      | "fields":[
      |    {"name":"str", "type":["null", {"type":"string","avro.java.string":"String"}], "default":null}
      | ]
      |}
    """.stripMargin.schema

  final val FooBarSchemaV2 =
    """
      |{
      | "type":"record",
      | "name":"AvroFooBar",
      | "namespace":"com.github.dnvriend.avro",
      | "fields":[
      |     {"name":"str", "type":["null", {"type":"string","avro.java.string":"String"}], "default":null},
      |     {"name":"x", "type": ["null", "int"], "default":null}
      | ]
      |}
    """.stripMargin.schema

  final val FooBarSchemaV3 =
    """
      |{
      | "type":"record",
      | "name":"AvroFooBar",
      | "namespace":"com.github.dnvriend.avro",
      | "fields":[
      |     {"name":"str", "type":["null", {"type":"string","avro.java.string":"String"}], "default":null},
      |     {"name":"x", "type": ["null", "int"], "default":null},
      |     {"name":"y", "type": ["null", "int"], "default":null}
      | ]
      |}
    """.stripMargin.schema

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
    val datumReader = new GenericDatumReader[GenericRecord](AvroFooBar.getClassSchema, AvroFooBar.getClassSchema)
    val record: GenericRecord = datumReader.read(null, decoder)
    record.get("str") shouldBe "this is a test"
  }

  def withGenericDatumAvroReader(writer: Schema = FooBarSchemaV1, reader: Schema = FooBarSchemaV1, data: Array[Byte] = AvroFooBarV1)(f: GenericRecord => Unit) = {
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(data, null)
    // you can actually give two different schemas to the Avro parser,
    // and it uses resolution rules
    // to translate data from the writer schema into the reader schema.
    // see: https://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protocol-buffers-thrift.html
    // see: http://avro.apache.org/docs/1.7.2/api/java/org/apache/avro/io/parsing/doc-files/parsing.html
    val datumReader = new GenericDatumReader[GenericRecord](writer, reader)
    val record: GenericRecord = datumReader.read(null, decoder)
    f(record)
  }

  // as stated by Martin Kleppmann:
  // "you need to know the exact schema with which the data was written (the writer’s schema)"
  // so the avro data should be the same version number as the writer's schema
  // so data and writer have the same version number
  // the reader however, doesn't

  "generic datum reader avro" should "deserialize v1 data with v1 reader" in withGenericDatumAvroReader(reader = FooBarSchemaV1) { v1 =>
    v1.get("str") shouldBe "this is a test v1"
  }

  it should "deserialize v1 data with v2 reader" in withGenericDatumAvroReader(reader = FooBarSchemaV2) { v1 =>
    v1.get("str") shouldBe "this is a test v1"
  }

  it should "deserialize v1 data with v3 reader" in withGenericDatumAvroReader(reader = FooBarSchemaV3) { v1 =>
    v1.get("str") shouldBe "this is a test v1"
  }

  it should "deserialize v2 data with v1 reader" in withGenericDatumAvroReader(reader = FooBarSchemaV1, writer = FooBarSchemaV2, data = AvroFooBarV2) { v2 =>
    v2.get("str") shouldBe "this is a test v2"
    v2.get("x") shouldBe null
  }

  it should "deserialize v2 data v2 reader" in withGenericDatumAvroReader(reader = FooBarSchemaV2, writer = FooBarSchemaV2, data = AvroFooBarV2) { v2 =>
    v2.get("str") shouldBe "this is a test v2"
    v2.get("x") shouldBe 1
  }

  it should "deserialize v2 data with v3 reader" in withGenericDatumAvroReader(reader = FooBarSchemaV3, writer = FooBarSchemaV2, data = AvroFooBarV2) { v2 =>
    v2.get("str") shouldBe "this is a test v2"
    v2.get("x") shouldBe 1
  }

  it should "deserialize v3 data with v1 reader" in withGenericDatumAvroReader(reader = FooBarSchemaV1, writer = FooBarSchemaV3, data = AvroFooBarV3) { v3 =>
    v3.get("str") shouldBe "this is a test v3"
    v3.get("x") shouldBe null
    v3.get("y") shouldBe null
  }

  it should "deserialize v3 data with v2 reader" in withGenericDatumAvroReader(reader = FooBarSchemaV2, writer = FooBarSchemaV3, data = AvroFooBarV3) { v3 =>
    v3.get("str") shouldBe "this is a test v3"
    v3.get("x") shouldBe 1
    v3.get("y") shouldBe null
  }

  it should "deserialize v3 data with v3 reader" in withGenericDatumAvroReader(reader = FooBarSchemaV3, writer = FooBarSchemaV3, data = AvroFooBarV3) { v3 =>
    v3.get("str") shouldBe "this is a test v3"
    v3.get("x") shouldBe 1
    v3.get("y") shouldBe 2
  }

  def withSpecificAvroReader[A >: Null](writer: Schema = FooBarSchemaV1, reader: Schema = FooBarSchemaV1, data: Array[Byte] = AvroFooBarV1)(f: A => Unit): Unit = {
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(data, null)
    val datumReader = new SpecificDatumReader[A](writer, reader)
    val record: A = datumReader.read(null, decoder)
    f(record)
  }

  "specific datum reader avro" should "read v1 data with v1 reader" in withSpecificAvroReader[AvroFooBar](data = AvroFooBarV1, writer = FooBarSchemaV1, reader = FooBarSchemaV1) { v1 =>
    v1.getStr shouldBe "this is a test v1"
  }

  it should "read v2 data with v1 reader" in withSpecificAvroReader[AvroFooBar](data = AvroFooBarV2, writer = FooBarSchemaV2, reader = FooBarSchemaV1) { v1 =>
    v1.getStr shouldBe "this is a test v2"
  }

  it should "read v3 data with v1 reader" in withSpecificAvroReader[AvroFooBar](data = AvroFooBarV3, writer = FooBarSchemaV3, reader = FooBarSchemaV1) { v1 =>
    v1.getStr shouldBe "this is a test v3"
  }

  def withGenericDatumJsonReader(writer: Schema = FooBarSchemaV1, reader: Schema = FooBarSchemaV1, json: String = JsonFooBarV1)(f: GenericRecord => Unit) = {
    println(s"parsing json: '$json'")
    val decoder: Decoder = DecoderFactory.get().jsonDecoder(writer, json)
    val datumReader = new GenericDatumReader[GenericRecord](writer, reader)
    val record: GenericRecord = datumReader.read(null, decoder)
    f(record)
  }

  "generic datum reader json" should "read v1 json data with v1 reader" in withGenericDatumJsonReader(json = JsonFooBarV1, writer = FooBarSchemaV1, reader = FooBarSchemaV1) { v1 =>
    v1.get("str") shouldBe "this is a test v1 json"
  }

  it should "read v1 data with v2 reader" in withGenericDatumJsonReader(json = JsonFooBarV1, writer = FooBarSchemaV1, reader = FooBarSchemaV2) { v1 =>
    v1.get("str") shouldBe "this is a test v1 json"
  }

  it should "read v1 data with v3 reader" in withGenericDatumJsonReader(json = JsonFooBarV1, writer = FooBarSchemaV1, reader = FooBarSchemaV3) { v1 =>
    v1.get("str") shouldBe "this is a test v1 json"
  }

  it should "read v2 json data with v1 reader" in withGenericDatumJsonReader(json = JsonFooBarV2, writer = FooBarSchemaV2, reader = FooBarSchemaV1) { v2 =>
    v2.get("str") shouldBe "this is a test v2 json"
    v2.get("x") shouldBe null
  }

  it should "read v2 json data with v2 reader" in withGenericDatumJsonReader(json = JsonFooBarV2, writer = FooBarSchemaV2, reader = FooBarSchemaV2) { v2 =>
    v2.get("str") shouldBe "this is a test v2 json"
    v2.get("x") shouldBe 1
  }

  it should "read v2 json data with v3 reader" in withGenericDatumJsonReader(json = JsonFooBarV2, writer = FooBarSchemaV2, reader = FooBarSchemaV3) { v2 =>
    v2.get("str") shouldBe "this is a test v2 json"
    v2.get("x") shouldBe 1
  }

  it should "read v3 json data with v1 reader" in withGenericDatumJsonReader(json = JsonFooBarV3, writer = FooBarSchemaV3, reader = FooBarSchemaV1) { v3 =>
    v3.get("str") shouldBe "this is a test v3 json"
    v3.get("x") shouldBe null
    v3.get("y") shouldBe null
  }

  it should "read v3 json data with v2 reader" in withGenericDatumJsonReader(json = JsonFooBarV3, writer = FooBarSchemaV3, reader = FooBarSchemaV2) { v3 =>
    v3.get("str") shouldBe "this is a test v3 json"
    v3.get("x") shouldBe 1
    v3.get("y") shouldBe null
  }

  it should "read v3 json data with v3 reader" in withGenericDatumJsonReader(json = JsonFooBarV3, writer = FooBarSchemaV3, reader = FooBarSchemaV3) { v3 =>
    v3.get("str") shouldBe "this is a test v3 json"
    v3.get("x") shouldBe 1
    v3.get("y") shouldBe 2
  }

  def withSpecificJsonReader[A >: Null](writer: Schema = FooBarSchemaV1, reader: Schema = FooBarSchemaV1, json: String = JsonFooBarV1)(f: A => Unit): Unit = {
    println(s"parsing json: '$json'")
    val decoder: Decoder = DecoderFactory.get().jsonDecoder(writer, json)
    val datumReader = new SpecificDatumReader[A](writer, reader)
    val record: A = datumReader.read(null, decoder)
    f(record)
  }

  "specific datum reader json" should "read v1 json data with v1 reader" in withSpecificJsonReader[AvroFooBar](json = JsonFooBarV1, writer = FooBarSchemaV1, reader = FooBarSchemaV1) { v1 =>
    v1.getStr shouldBe "this is a test v1 json"
  }

  it should "read v2 json data with v1 reader" in withSpecificJsonReader[AvroFooBar](json = JsonFooBarV2, writer = FooBarSchemaV2, reader = FooBarSchemaV1) { v1 =>
    v1.getStr shouldBe "this is a test v2 json"
  }

  it should "read v3 json data with v1 reader" in withSpecificJsonReader[AvroFooBar](json = JsonFooBarV3, writer = FooBarSchemaV3, reader = FooBarSchemaV1) { v1 =>
    v1.getStr shouldBe "this is a test v3 json"
  }
}
