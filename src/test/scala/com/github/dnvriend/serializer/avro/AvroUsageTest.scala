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

  it should "get the schema from fqcn" in {
    val clazz: Class[_] = Class.forName("com.github.dnvriend.avro.AvroFooBar")
    val method = clazz.getMethod("getClassSchema")
    val result = method.invoke(null)
    result shouldBe a[Schema]
    result.toString shouldBe """{"type":"record","name":"AvroFooBar","namespace":"com.github.dnvriend.avro","fields":[{"name":"str","type":"string"}]}"""
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
    baos.size() shouldBe 15
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
    out.size() shouldBe 15
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
    bytes.size shouldBe 15

    val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val reader = new SpecificDatumReader[AnyRef](AvroFooBar.getClassSchema)
    val objectRead = reader.read(null, decoder)
    objectRead.asInstanceOf[AvroFooBar].getStr.toString shouldBe "this is a test"
  }
}
