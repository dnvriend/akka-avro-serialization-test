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
import com.sksamuel.avro4s.{ FromRecord, SchemaFor, ToRecord }
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, GenericDatumWriter, GenericRecord }
import org.apache.avro.io.{ BinaryEncoder, Decoder, DecoderFactory, EncoderFactory }
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter }

trait AvroMarker {
  def avsc: String
  def schema: Schema = new Schema.Parser().parse(avsc)
  def record: GenericRecord
}

class AvroMarkerSerializer extends SerializerWithStringManifest {
  override def identifier: Int = 101

  // the manifest for the serializer
  override def manifest(o: AnyRef): String = {
    println(s"AvroMarkerSerializer.manifest: generating manifest (type-hint) for the fromBinary method: '${o.getClass.getName}'")
    o.getClass.getName
  }

  override def toBinary(o: AnyRef): Array[Byte] = {
    println("AvroMarkerSerializer.toBinary: converting to binary array: " + o.getClass.getName)
    //    o match {
    //      case marker: AvroMarker[_] =>
    //        val schema: Schema = marker.schema
    //        val out = new ByteArrayOutputStream()
    //        val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    //        val writer = new GenericDatumWriter[GenericRecord](schema)
    //        writer.write(marker.toRecord, encoder)
    //        encoder.flush()
    //        out.close()
    //        out.toByteArray
    //    }
    ???
  }

  def getSchema(fqcn: String): Schema = {
    val clazz: Class[_] = Class.forName(fqcn)
    val method = clazz.getMethod("schema")
    method.invoke(null).asInstanceOf[Schema]
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    println(s"AvroMarkerSerializer.fromBinary: converting from binary with manifest: '$manifest'")
    val schema = getSchema(manifest)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val reader = new SpecificDatumReader[AnyRef](schema)
    reader.read(null, decoder)
  }
}
