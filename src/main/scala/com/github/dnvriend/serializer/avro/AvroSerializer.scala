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

import akka.serialization.SerializerWithStringManifest
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io._
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter, SpecificRecordBase }

/**
 * Serializes Avro generated org.apache.avro.specific.SpecificRecordBase to the journal
 * as byte array. It assumes that Java generated Avro classes are on the classpath
 */
class AvroSerializer extends SerializerWithStringManifest {
  override def identifier: Int = 100

  // the manifest for the serializer
  override def manifest(o: AnyRef): String = {
    println(s"AvroSerializer.manifest: generating manifest (type-hint) for the fromBinary method: '${o.getClass.getName}'")
    o.getClass.getName
  }

  override def toBinary(o: AnyRef): Array[Byte] = {
    println("AvroSerializer.toBinary: converting to binary array: " + o.getClass.getName)
    o match {
      case datum: SpecificRecordBase =>
        val schema: Schema = datum.getSchema
        val out = new ByteArrayOutputStream()
        val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
        val writer = new SpecificDatumWriter[GenericRecord](schema)
        writer.write(datum, encoder)
        encoder.flush()
        out.close()
        out.toByteArray
    }
  }

  def getSchema(fqcn: String): Schema = {
    val clazz: Class[_] = Class.forName(fqcn)
    val method = clazz.getMethod("getClassSchema")
    method.invoke(null).asInstanceOf[Schema]
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    println(s"AvroSerializer.fromBinary: converting from binary with manifest: '$manifest'")
    val schema = getSchema(manifest)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val reader = new SpecificDatumReader[AnyRef](schema)
    reader.read(null, decoder)
  }
}