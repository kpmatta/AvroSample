package com.data

import java.io._

import org.apache.avro._
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumReader, DatumWriter}

object AvroSample {
  def main(args: Array[String]): Unit = {
    // Read schema
    val schemaPath = "src/main/resources/user.avsc"
    val avroSchema = getAvroSchema(schemaPath)

    // Create avro file
    val avroFilePath = "users.avro"
    SeializeAvroData(avroSchema, avroFilePath)

    // Read from avro file
    DeSerializeAvroData(avroSchema, avroFilePath)
  }

  // Get avro schema from path
  def getAvroSchema(schemaPath: String ): Schema = {

    // Read schema file from resource
    val schema = new Schema.Parser().parse(new File(schemaPath))
    println(schema)
    schema
  }


  // Save avro file
  def SeializeAvroData(schema: Schema, outputFile: String ): Unit = {

    val user1: GenericRecord = new GenericData.Record(schema)
    user1.put("name", "Alyssa")
    user1.put("favorite_number", 256)

    val user2: GenericRecord = new GenericData.Record(schema)
    user2.put("name", "Ben")
    user2.put("favorite_number", 7)
    user2.put("favorite_color", "red")

    // Serializing to disk
    val file: File = new File(outputFile)
    val datumWriter : DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)

    // Create a new file
    dataFileWriter.create(schema, file)

    // Or append to existing file
    //dataFileWriter.appendTo(file)

    dataFileWriter.append(user1)
    dataFileWriter.append(user2)
    dataFileWriter.close()
  }

  // Read data from avro file and schema
  def DeSerializeAvroData(schema: Schema, avroFilePath: String): Unit = {

    val file: File = new File(avroFilePath)
    val datumReader : DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
    val dataFileReader: DataFileReader[GenericRecord] =  new DataFileReader[GenericRecord](file, datumReader)

    var user : GenericRecord = null

    while ( dataFileReader.hasNext) {
      user = dataFileReader.next(user)
      println(user)
    }
  }
}
