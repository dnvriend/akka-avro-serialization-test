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

import java.io.{ ByteArrayOutputStream, OutputStream }

import com.github.dnvriend.TestSpec
import com.github.dnvriend.avro.AvroFooBar
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{ GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord }
import org.apache.avro.io.{ BinaryEncoder, Decoder, DecoderFactory, EncoderFactory }
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter }

class AvroUsageTest extends TestSpec {

  def withWriter(writerSchema: Schema)(f: Record => (GenericRecord => Unit) => Unit): Array[Byte] = {
    val record = new Record(writerSchema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    val writer = new GenericDatumWriter[GenericRecord](writerSchema)
    f(record)(writer.write(_: GenericRecord, encoder))
    encoder.flush()
    out.close()
    out.toByteArray
  }

  def AvroFooBarV1: Array[Byte] = withWriter(AvroFooBarSchemaV1.schema) { record => writer =>
    record.put("str", "this is a test v1")
    writer(record)
  }

  def AvroFooBarV2: Array[Byte] = withWriter(AvroFooBarSchemaV2.schema) { record => writer =>
    record.put("str", "this is a test v2")
    record.put("x", 1)
    writer(record)
  }

  def AvroFooBarV3: Array[Byte] = withWriter(AvroFooBarSchemaV3.schema) { record => writer =>
    record.put("str", "this is a test v3")
    record.put("x", 1)
    record.put("y", 2)
    writer(record)
  }

  final val AvroFooBarSchemaV1 =
    """
      |{
      | "type":"record",
      | "name":"AvroFooBar",
      | "namespace":"com.github.dnvriend.avro",
      | "fields":[
      |    {"name":"str", "type":["null", {"type":"string","avro.java.string":"String"}], "default":null}
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
      |     {"name":"str", "type":["null", {"type":"string","avro.java.string":"String"}], "default":null},
      |     {"name":"x", "type": ["null", "int"], "default":null}
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
      |     {"name":"str", "type":["null", {"type":"string","avro.java.string":"String"}], "default":null},
      |     {"name":"x", "type": ["null", "int"], "default":null},
      |     {"name":"y", "type": ["null", "int"], "default":null}
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
    val datumReader = new GenericDatumReader[GenericRecord](AvroFooBar.getClassSchema, AvroFooBar.getClassSchema)
    val record: GenericRecord = datumReader.read(null, decoder)
    record.get("str") shouldBe "this is a test"
  }

  def withRecord(writer: String = AvroFooBarSchemaV1, reader: String = AvroFooBarSchemaV1, data: Array[Byte] = AvroFooBarV1)(f: GenericRecord => Unit) = {
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(data, null)
    // you can actually give two different schemas to the Avro parser,
    // and it uses resolution rules
    // to translate data from the writer schema into the reader schema.
    // see: https://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protocol-buffers-thrift.html
    // see: http://avro.apache.org/docs/1.7.2/api/java/org/apache/avro/io/parsing/doc-files/parsing.html
    val datumReader = new GenericDatumReader[GenericRecord](writer.schema, reader.schema)
    val record: GenericRecord = datumReader.read(null, decoder)
    f(record)
  }

  // as stated by Martin Kleppmann:
  // "you need to know the exact schema with which the data was written (the writer’s schema)"
  // so the avro data should be the same version number as the writer's schema
  // so data and writer have the same version number
  // the reader however, doesn't

  it should "deserialize v1 data with v1 reader" in withRecord(reader = AvroFooBarSchemaV1) { v1 =>
    v1.get("str") shouldBe "this is a test v1"
  }

  it should "deserialize v1 data with v2 reader" in withRecord(reader = AvroFooBarSchemaV2) { v1 =>
    v1.get("str") shouldBe "this is a test v1"
  }

  it should "deserialize v1 data with v3 reader" in withRecord(reader = AvroFooBarSchemaV3) { v1 =>
    v1.get("str") shouldBe "this is a test v1"
  }

  it should "deserialize v2 data with v1 reader" in withRecord(reader = AvroFooBarSchemaV1, writer = AvroFooBarSchemaV2, data = AvroFooBarV2) { v2 =>
    v2.get("str") shouldBe "this is a test v2"
  }

  it should "deserialize v2 data v2 reader" in withRecord(reader = AvroFooBarSchemaV2, writer = AvroFooBarSchemaV2, data = AvroFooBarV2) { v2 =>
    v2.get("str") shouldBe "this is a test v2"
    v2.get("x") shouldBe 1
  }

  it should "deserialize v2 data with v3 reader" in withRecord(reader = AvroFooBarSchemaV3, writer = AvroFooBarSchemaV2, data = AvroFooBarV2) { v2 =>
    v2.get("str") shouldBe "this is a test v2"
    v2.get("x") shouldBe 1
  }

  it should "deserialize v3 data with v1 reader" in withRecord(reader = AvroFooBarSchemaV1, writer = AvroFooBarSchemaV3, data = AvroFooBarV3) { v3 =>
    v3.get("str") shouldBe "this is a test v3"
  }

  it should "deserialize v3 data with v2 reader" in withRecord(reader = AvroFooBarSchemaV2, writer = AvroFooBarSchemaV3, data = AvroFooBarV3) { v3 =>
    v3.get("str") shouldBe "this is a test v3"
    v3.get("x") shouldBe 1
  }

  it should "deserialize v3 data with v3 reader" in withRecord(reader = AvroFooBarSchemaV3, writer = AvroFooBarSchemaV3, data = AvroFooBarV3) { v3 =>
    v3.get("str") shouldBe "this is a test v3"
    v3.get("x") shouldBe 1
    v3.get("y") shouldBe 2
  }

}
