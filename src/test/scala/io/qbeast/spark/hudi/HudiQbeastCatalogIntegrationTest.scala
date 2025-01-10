/*
 * Copyright 2021 Qbeast Analytics, S.L.
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
package io.qbeast.spark.hudi

import io.qbeast.table.QbeastTable
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.avro.generic.GenericData
import org.apache.avro.Schema
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.model.DefaultHoodieRecordPayload
import org.apache.hudi.common.model.HoodieAvroPayload
import org.apache.hudi.common.model.HoodieAvroRecord
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.model.HoodieKey
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.common.util.CommitUtils
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.MetadataPartitionType
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.DataSourceWriteOptions.SET_NULL_FOR_MISSING_COLUMNS
import org.apache.hudi.HoodieCLIUtils
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import java.nio.file.Paths
import java.util
import scala.collection.JavaConverters._
import scala.util.Random

object HudiUtils {

  def createHoodieRecordFromRow(row: Row, schema: Schema): HoodieRecord[HoodieAvroPayload] = {
    val record: GenericData.Record = new GenericData.Record(schema)

    row.schema.fields.zipWithIndex.foreach { case (field, index) =>
      record.put(field.name, row.get(index))
    }

    val hoodieKey = new HoodieKey(row.getAs[Any]("id").toString, "")
    val payload = new HoodieAvroPayload(Option.of(record))

    new HoodieAvroRecord(hoodieKey, payload)
  }

}

case class Student(id: Int, name: String, age: Int)

object StudentGenerator {

  private val names =
    List("Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Hannah", "Isaac", "Jack")

  // Method to generate a configurable number of students
  def generateStudents(count: Int): Seq[Student] = {
    1.to(count).map { i =>
      val id = i
      val name = names(Random.nextInt(names.length)) // Randomly select a name from the list
      val age = Random.nextInt(30) + 18 // Random age between 18 and 47
      Student(id, name, age)
    }
  }

}

class HudiQbeastCatalogIntegrationTest extends QbeastIntegrationTestSpec {

  def createMetaClient(jsc: JavaSparkContext, basePath: String): HoodieTableMetaClient = {
    HoodieTableMetaClient.builder
      .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
      .setBasePath(basePath)
      .build
  }

  def createTestData(spark: SparkSession, numerOfRows: Int): DataFrame = {
    import spark.implicits._
    StudentGenerator.generateStudents(numerOfRows).toDF()
  }

  def createHoodieRecordsFromDataFrame(
      df: DataFrame,
      schema: Schema): JavaRDD[HoodieRecord[HoodieAvroPayload]] = {
    df.rdd.map(row => HudiUtils.createHoodieRecordFromRow(row, schema)).toJavaRDD()
  }

  val hudiSparkConf: SparkConf = new SparkConf()
    .setMaster("local[8]")
    .set("spark.sql.extensions", "io.qbeast.sql.HudiQbeastSparkSessionExtension")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .set("spark.qbeast.tableFormat", "hudi")
    .set("spark.qbeast.index.defaultCubeSize", "100")

  // .set("spark.sql.catalog.qbeast_catalog", "io.qbeast.catalog.QbeastCatalog")

  "Hudi Qbeast Catalog" should
    "create a table" in withExtendedSparkAndTmpDir(hudiSparkConf) { (spark, tmpDir) =>
      val data = createTestData(spark, 1000)
      val tableName: String = "hudi_table"
      data.write.format("hudi").saveAsTable(tableName)

      val tables = spark.sessionState.catalog.listTables("default")
      tables.size shouldBe 1
      println(tables)

      val hudiTable = spark.read.table(tableName)
      hudiTable.printSchema()
    }

  it should
    "create a new commit with custom metadata" in withExtendedSparkAndTmpDir(hudiSparkConf) {
      (spark, tmpDir) =>
        val tableName: String = "hudi_table"
        val directoryPath = s"spark-warehouse/$tableName"
        removeDirectory(directoryPath)

        val data = createTestData(spark, 1000)

        data.write
          .format("hudi")
          .option("hoodie.table.name", tableName)
          .option("hoodie.table.type", "COPY_ON_WRITE")
          .option("hoodie.metadata.enable", "true")
          .option("hoodie.metadata.index.column.stats.enable", "true")
          .option("hoodie.metadata.index.bloom.filter.enable", "true")
          .option("hoodie.metadata.record.index.enable", "true")
          .saveAsTable(tableName)

        val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(spark, tableName)
        val basePath = hoodieCatalogTable.tableLocation

        val jsc = new JavaSparkContext(spark.sparkContext)
        val metaClient = createMetaClient(jsc, basePath)

        println(metaClient.getTableConfig)

        val writeConfig = HoodieWriteConfig
          .newBuilder()
          .withPath(basePath)
          .forTable(hoodieCatalogTable.tableName)
          .build()

        val engineContext = new HoodieSparkEngineContext(jsc)

        val hoodieTable: HoodieSparkTable[DefaultHoodieRecordPayload] =
          HoodieSparkTable.create(writeConfig, engineContext, metaClient)

        // alternative: hoodieTable.getMetadataWriter(commitTime)
        val metadataWriter = SparkHoodieBackedTableMetadataWriter.create(
          HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()),
          writeConfig,
          engineContext)
        println(metadataWriter.isInitialized)

        val commitMetadata = new HoodieCommitMetadata()
        commitMetadata.addMetadata("new_key_1", "new_value_1_qbeast")
        commitMetadata.addMetadata("new_key_2", "new_value_2_qbeast")
        val extraMetadata = commitMetadata.getExtraMetadata
        extraMetadata.put("additional_key", "additional_value")
        val serializedCommitMetadata = commitMetadata.toJsonString
        println(serializedCommitMetadata)

        var timeline = hoodieTable.getActiveTimeline
        println(timeline)
        val instantTime = metaClient.createNewInstantTime()
        val instantGenerator = metaClient.getTimelineLayout.getInstantGenerator
        val hoodieInstant = instantGenerator.createNewInstant(
          State.REQUESTED,
          HoodieTimeline.COMMIT_ACTION,
          instantTime)
        timeline.createNewInstant(hoodieInstant)
        timeline.saveAsComplete(hoodieInstant, Option.of(commitMetadata.toJsonString.getBytes))

        timeline = timeline.reload()
        print(timeline)
        timeline.countInstants() shouldBe 2

//        val metadataDF = spark.read.format("hudi").load(s"$basePath/.hoodie/metadata")
//        metadataDF.printSchema()
    }

  it should
    "commit data to the table" in withExtendedSparkAndTmpDir(hudiSparkConf) { (spark, tmpDir) =>
      val tableName: String = "hudi_table"
      val directoryPath = s"spark-warehouse/$tableName"
      removeDirectory(directoryPath)
//      val data = createTestData(spark)
//      val basePath = s"file:///$tmpDir/hudi_table"
//      val tableName: String = "hudi_table"
//      data.write.format("hudi").option("hoodie.table.name", tableName).save(basePath)

      val data = createTestData(spark, 1000)

      data.write
        .format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.table.type", "COPY_ON_WRITE")
        .option("hoodie.metadata.enable", "true")
        .option("hoodie.metadata.index.column.stats.enable", "true")
        .option("hoodie.metadata.index.bloom.filter.enable", "true")
        .option("hoodie.metadata.record.index.enable", "true")
        .saveAsTable(tableName)

      val sparkTable = spark.read.table(tableName)
      sparkTable.printSchema()

      val writeSchema = AvroConversionUtils
        .convertStructTypeToAvroSchema(sparkTable.schema, "", "")

      println(writeSchema.toString(true))

      val jsc = new JavaSparkContext(spark.sparkContext)

      val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(spark, tableName)

      val engineContext = new HoodieSparkEngineContext(jsc)
      val writeConfig = HoodieWriteConfig
        .newBuilder()
        .withSchema(writeSchema.toString)
        .withPath(hoodieCatalogTable.tableLocation)
        .forTable(hoodieCatalogTable.tableName)
        .build()
      val writeClient =
        new SparkRDDWriteClient[HoodieAvroPayload](engineContext, writeConfig)
      println(writeClient)

      import spark.implicits._
      val newData: DataFrame =
        Seq((11, "Alice", 66), (12, "Bob", 77)).toDF("id", "name", "age")

      val hoodieRecords: JavaRDD[HoodieRecord[HoodieAvroPayload]] = {
        createHoodieRecordsFromDataFrame(newData, writeSchema)
      }

      val commitActionType =
        CommitUtils.getCommitActionType(
          WriteOperationType.BULK_INSERT,
          hoodieCatalogTable.tableType)
      val instantTime = writeClient.createNewInstantTime()

      writeClient.startCommitWithTime(instantTime, commitActionType)

      println(instantTime)
      // bulkInsert already writes all the necessary files
      val writeStatuses = writeClient.bulkInsert(hoodieRecords, instantTime)
      println(writeStatuses.collect().toString)

      // writeClient.runAnyPendingCompactions()
      // AlterTableCommand.scala DataSourceUtils.scala HoodieCLIUtils TestHoodieMergeOnReadTable.java

      val hoodieTable =
        HoodieSparkTable.create(writeConfig, engineContext, hoodieCatalogTable.metaClient)

      val timeline = hoodieTable.getActiveTimeline
      println(timeline)
      timeline.countInstants() shouldBe 2

      spark.catalog.refreshTable(tableName)
      spark.read.table(tableName).show(truncate = false)
    }

  it should
    "read and print the meatadata" in withExtendedSparkAndTmpDir(hudiSparkConf) {
      (spark, tmpDir) =>
        val tableName: String = "hudi_table"
        val directoryPath = s"spark-warehouse/$tableName"
        removeDirectory(directoryPath)

        val data = createTestData(spark, 1000)

        data.write
          .format("hudi")
          .option("hoodie.table.name", tableName)
          .option("hoodie.table.type", "COPY_ON_WRITE")
          .option("hoodie.metadata.enable", "true")
          .option("hoodie.metadata.index.column.stats.enable", "true")
          .option("hoodie.metadata.index.bloom.filter.enable", "true")
          .option("hoodie.metadata.record.index.enable", "true")
          .saveAsTable(tableName)

        val jsc = new JavaSparkContext(spark.sparkContext)

        val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(spark, tableName)
        val basePath = hoodieCatalogTable.tableLocation

        val engineContext = new HoodieSparkEngineContext(jsc)
        val writeConfig = HoodieWriteConfig
          .newBuilder()
          .withPath(basePath)
          .forTable(hoodieCatalogTable.tableName)
          .build()

        val hoodieTable =
          HoodieSparkTable.create(writeConfig, engineContext, hoodieCatalogTable.metaClient)

        val metaReader = hoodieTable.getMetadataTable

        val metaClient = hoodieTable.getMetaClient
        println(metaClient.getTableType)
        println(metaClient.getMetaPath)
        println(metaClient.getTableConfig.isTablePartitioned)
        println(metaClient.getTableConfig.getTableVersion)
        println(metaClient.getTableConfig.getPayloadClass)
        println(metaClient.getTableConfig.getMetadataPartitions)
        println(metaClient.getTableConfig.isMetadataTableAvailable)

        println(metaClient.getActiveTimeline.getAllCommitsTimeline.lastInstant().get())

        val metaClient1 = createMetaClient(jsc, basePath)
        println(metaClient1.getTableType)

// This gets the metadata table
//        val tableMetadata = new HoodieBackedTableMetadata(
//          engineContext,
//          metaClient.getStorage,
//          writeConfig.getMetadataConfig,
//          writeConfig.getBasePath)
//
//        println("This is potentially the metadata table:")
//        println(tableMetadata.getMetadataMetaClient.getTableType)
//        println(tableMetadata.getMetadataMetaClient.getTableConfig.isTablePartitioned)
//        println(tableMetadata.getMetadataMetaClient.getTableConfig.getDatabaseName)

        println("------- Metadata writer")

        val metadataWriter = SparkHoodieBackedTableMetadataWriter.create(
          engineContext.getStorageConf,
          writeConfig,
          engineContext)
        println(metadataWriter)

//        val lastInstant = metaClient.getActiveTimeline.getAllCommitsTimeline.lastInstant().get()
//        val lastInstantDetails = metaClient.getActiveTimeline.getInstantDetails(lastInstant).get

        println(metaClient.getTableConfig.getMetadataPartitions)
        val instantTime = metaClient.createNewInstantTime()
        println(instantTime)

        // val metadataWriter = hoodieTable.getMetadataWriter(instantTime).get()

        // println(metadataWriter)
        // println(metadataWriter.isInitialized)

        val bloomFiltersList: java.util.List[MetadataPartitionType] =
          new util.ArrayList[MetadataPartitionType]()
        bloomFiltersList.add(MetadataPartitionType.RECORD_INDEX)

        println(metaClient.getTableConfig.getMetadataPartitions)
        metaClient.reloadActiveTimeline()
        println(metaClient.getTableConfig.getMetadataPartitions)
        println(metaClient.getTableConfig.isMetadataTableAvailable)

        val partitionPath = new StoragePath(basePath)
        println(partitionPath.toString)

        val fileStatuses = metaReader.getAllFilesInPartition(partitionPath)
        println(fileStatuses)

        val partitionFilePairs = fileStatuses.asScala.map { fileStatus =>
          Pair.of("", fileStatus.getPath.getName)
        }.asJava

        val columnStatsMap = metaReader.getColumnStats(partitionFilePairs, "age")

        println(columnStatsMap)

        // Process the statistics
        columnStatsMap.asScala.foreach { case (partitionFilePair, columnStats) =>
          val partition = partitionFilePair.getLeft
          val fileName = partitionFilePair.getRight
          val minValue = columnStats.getMinValue
          val maxValue = columnStats.getMaxValue

          println(
            s"Partition: $partition, File: $fileName, MinValue: $minValue, MaxValue: $maxValue")
        }

        // val metadataDF = spark.read.format("hudi").load(s"$basePath/.hoodie/metadata")
        // metadataDF.printSchema()

    }

  it should
    "write qbeast files" in withExtendedSparkAndTmpDir(hudiSparkConf) { (spark, tmpDir) =>
      val tableName: String = "hudi_table_v1"
      val currentPath = Paths.get("").toAbsolutePath.toString
      val basePath = s"$currentPath/spark-warehouse/$tableName"

      removeDirectory(basePath)

      val hudiOptions = Map(
        "columnsToIndex" -> "id",
        "hoodie.table.name" -> tableName,
        "hoodie.metadata.enable" -> "true"
        // "hoodie.file.index.enable" -> "true",
        // "hoodie.metadata.index.bloom.filter.enable" -> "true",
        // "hoodie.metadata.index.column.stats.enable" -> "true"
        // "hoodie.metadata.record.index.enable" -> "true",

        // "hoodie.populate.meta.fields" -> "false",
        // "hoodie.table.recordkey.fields" -> "id",
        // "hoodie.sql.bulk.insert.enable " -> "true",
        // "hoodie.sql.insert.mode" -> "non-strict"

        // "hoodie.spark.sql.optimized.writes.enable" -> "true"

        // "hoodie.keep.max.commits" -> "5",
        // "hoodie.keep.min.commits" -> "1",
        // "hoodie.clean.automatic" -> "false",

        // "hoodie.archive.automatic" -> "true",
        // "hoodie.commits.archival.batch" -> "10")
      )

      val tableFormat = "qbeast"

//      val data = createTestData(spark, 100)
//      data.write
//        .format(tableFormat)
//        .mode("overwrite")
//        .options(hudiOptions)
//        .save(basePath)

//      spark.read
//        .format(tableFormat)
//        .load(basePath)
//        .show(1000, truncate = false)
//
//      spark.read
//        .format("hudi")
//        .load(basePath)
//        .show(1000, truncate = false)

//      (1 to 5).foreach { _ =>
//        val data2 = createTestData(spark, 10)
//        data2.write
//          .format(tableFormat)
//          .mode("append")
//          .options(hudiOptions)
//          .save(basePath)
//      }

      // Define schema
      val schema = StructType(
        Seq(
          StructField("_hoodie_commit_time", StringType, nullable = false),
          StructField("_hoodie_commit_seqno", StringType, nullable = false),
          StructField("_hoodie_record_key", StringType, nullable = false),
          StructField("_hoodie_partition_path", StringType, nullable = true),
          StructField("_hoodie_file_name", StringType, nullable = false),
          StructField("id", IntegerType, nullable = false),
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false)))

      // Create data
      val data = Seq(
        Row(
          "20250110111943697",
          "20250110111943697_0_1",
          "20250110111943697_0_1",
          "",
          "c93ddd88-9cbe-443b-904c-1e09f562cf68-0_0-27-0_20250110111943697.parquet",
          1,
          "Eva",
          29),
        Row(
          "20250110111943697",
          "20250110111943697_0_2",
          "20250110111943697_0_2",
          "",
          "c93ddd88-9cbe-443b-904c-1e09f562cf68-0_0-27-0_20250110111943697.parquet",
          2,
          "Alice",
          45),
        Row(
          "20250110111943697",
          "20250110111943697_0_3",
          "20250110111943697_0_3",
          "",
          "c93ddd88-9cbe-443b-904c-1e09f562cf68-0_0-27-0_20250110111943697.parquet",
          3,
          "Hannah",
          29),
        Row(
          "20250110111943697",
          "20250110111943697_0_4",
          "20250110111943697_0_4",
          "",
          "c93ddd88-9cbe-443b-904c-1e09f562cf68-0_0-27-0_20250110111943697.parquet",
          4,
          "David",
          31),
        Row(
          "20250110111943697",
          "20250110111943697_0_5",
          "20250110111943697_0_5",
          "",
          "c93ddd88-9cbe-443b-904c-1e09f562cf68-0_0-27-0_20250110111943697.parquet",
          5,
          "Hannah",
          28),
        Row(
          "20250110111943697",
          "20250110111943697_0_6",
          "20250110111943697_0_6",
          "",
          "c93ddd88-9cbe-443b-904c-1e09f562cf68-0_0-27-0_20250110111943697.parquet",
          6,
          "Isaac",
          21),
        Row(
          "20250110111943697",
          "20250110111943697_0_7",
          "20250110111943697_0_7",
          "",
          "c93ddd88-9cbe-443b-904c-1e09f562cf68-0_0-27-0_20250110111943697.parquet",
          7,
          "Isaac",
          20),
        Row(
          "20250110111943697",
          "20250110111943697_0_8",
          "20250110111943697_0_8",
          "",
          "c93ddd88-9cbe-443b-904c-1e09f562cf68-0_0-27-0_20250110111943697.parquet",
          8,
          "Charlie",
          47),
        Row(
          "20250110111943697",
          "20250110111943697_0_9",
          "20250110111943697_0_9",
          "",
          "c93ddd88-9cbe-443b-904c-1e09f562cf68-0_0-27-0_20250110111943697.parquet",
          9,
          "David",
          33),
        Row(
          "20250110111943697",
          "20250110111943697_0_10",
          "20250110111943697_0_10",
          "",
          "c93ddd88-9cbe-443b-904c-1e09f562cf68-0_0-27-0_20250110111943697.parquet",
          10,
          "Isaac",
          25))

      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write
        .format(tableFormat)
        .mode("overwrite")
        .options(hudiOptions)
        .save(basePath)

      println("--- COUNTING ROWS ---")
      println(
        spark.read
          .format(tableFormat)
          .load(basePath)
          .count())

      println("--- READING ---")
      spark.read
        .format(tableFormat)
        .load(basePath)
        .show(numRows = 10, truncate = false)

      println("--- SAMPLING ---")
      spark.read
        .format(tableFormat)
        .load(basePath)
        .sample(0.1)
        .show(numRows = 10, truncate = false)

    }

  it should
    "read qbeast hudi metadata files" in withExtendedSparkAndTmpDir(hudiSparkConf) {
      (spark, tmpDir) =>
        val tableName: String = "hudi_table"
        val currentPath = Paths.get("").toAbsolutePath.toString
        val basePath = s"$currentPath/spark-warehouse/$tableName"

        val metadataDF = spark.read.format("hudi").load(s"$basePath/.hoodie/metadata")
        metadataDF.printSchema()
        metadataDF.show(numRows = 100, truncate = false)

        val tableFormat = "qbeast"

        spark.read
          .format(tableFormat)
          .load(basePath)
          .sample(0.1)
          .show(numRows = 10, truncate = false)

        println(
          spark.read
            .format(tableFormat)
            .load(basePath)
            .count())
    }

  it should
    "extend qbeast metadata files" in withExtendedSparkAndTmpDir(hudiSparkConf) {
      (spark, tmpDir) =>
        val tableName: String = "hudi_table"
        val currentPath = Paths.get("").toAbsolutePath.toString
        val basePath = s"$currentPath/spark-warehouse/$tableName"
        val metadataPath = s"$basePath/.hoodie/metadata"

        val metadataDF = spark.read.format("hudi").load(metadataPath)
        println(metadataDF.schema)
        metadataDF.printSchema()
        // metadataDF.show(numRows = 100, truncate = false)

        import org.apache.spark.sql.types._

        val extendedSchema = StructType(
          metadataDF.schema.fields ++ Seq(StructField(
            "qbeastMetadata",
            StructType(Seq(
              StructField("fileName", StringType, nullable = false),
              StructField("revision", IntegerType, nullable = false),
              StructField(
                "blocks",
                StructType(Seq(
                  StructField("cubeId", IntegerType, nullable = false),
                  StructField("minWeight", IntegerType, nullable = false),
                  StructField("maxWeight", IntegerType, nullable = false),
                  StructField("elementCount", IntegerType, nullable = false))),
                nullable = false))),
            nullable = true)))

        println(extendedSchema)

        val newData = Seq(
          Row(
            "key1", // key
            2, // type
            null, // filesystemMetadata
            null, // BloomFilterMetadata
            null, // ColumnStatsMetadata
            null // recordIndexMetadata
//            Row( // qbeastMetadata
//              "file1", // fileName
//              1, // revision
//              Row( // blocks
//                123, // cubeId
//                10, // minWeight
//                20, // maxWeight
//                100 // elementCount
//              ))
          ))

        val newDataDF =
          spark.createDataFrame(spark.sparkContext.parallelize(newData), metadataDF.schema)

        newDataDF.write
          .format("hudi")
          .mode("append")
          .save(metadataPath)

        val metadataDF2 = spark.read.format("hudi").load(s"$basePath/.hoodie/metadata")
        metadataDF2.printSchema()
        metadataDF2.show(numRows = 100, truncate = false)
    }

  it should
    "optimize qbeast table" in withExtendedSparkAndTmpDir(hudiSparkConf) { (spark, tmpDir) =>
      val tableName: String = "hudi_table_optimize"
      val currentPath = Paths.get("").toAbsolutePath.toString
      val basePath = s"$currentPath/spark-josep/$tableName"

      removeDirectory(basePath)

      val hudiOptions = Map(
        "hoodie.table.name" -> tableName,
        "hoodie.metadata.enable" -> "true",
        "hoodie.file.index.enable" -> "true",
        "hoodie.metadata.index.column.stats.enable" -> "true")

      val data = createTestData(spark, 1000)
      data.write
        .format("qbeast")
        .mode("overwrite")
        .options(hudiOptions)
        .option("columnsToIndex", "id")
        .save(basePath)

      val data2 = createTestData(spark, 500)
      data2.write
        .format("qbeast")
        .mode("append")
        .options(hudiOptions)
        .option("columnsToIndex", "id")
        .save(basePath)

      val data3 = createTestData(spark, 250)
      data3.write
        .format("qbeast")
        .mode("overwrite")
        .options(hudiOptions)
        .option("columnsToIndex", "id")
        .save(basePath)

      val qbeastTable = QbeastTable.forPath(spark, basePath)
      println(qbeastTable.getIndexMetrics)
      qbeastTable.optimize()

      println("Appending 50 rows")
      val data4 = createTestData(spark, 50)
      data4.write
        .format("qbeast")
        .mode("append")
        .options(hudiOptions)
        .option("columnsToIndex", "id")
        .save(basePath)

      println("Querying. Total rows:")
      println(
        spark.read
          .format("qbeast")
          .load(basePath)
          .count())

      spark.read
        .format("qbeast")
        .load(basePath)
        .sample(0.1)
        .show(numRows = 10, truncate = false)

    }

  it should
    "cluster hudi table" in withExtendedSparkAndTmpDir(hudiSparkConf) { (spark, tmpDir) =>
      val tableName: String = "hudi_table_cluster"
      val currentPath = Paths.get("").toAbsolutePath.toString
      val basePath = s"$currentPath/spark-warehouse/$tableName"

      removeDirectory(basePath)

      val data = createTestData(spark, 1000)
      data.write
        .format("hudi")
        .option("hoodie.clustering.inline.max.commits", "2")
        .option("hoodie.clustering.inline", "true")
        .option(HoodieWriteConfig.TBL_NAME.key, tableName)
        .mode("overwrite")
        .save(basePath)

      val newData = createTestData(spark, 1000)
      newData.write
        .format("hudi")
        .option("hoodie.clustering.inline.max.commits", "2")
        .option("hoodie.clustering.inline", "true")
        .option(HoodieWriteConfig.TBL_NAME.key, tableName)
        .mode("append")
        .save(basePath)

      spark.read
        .format("hudi")
        .load(basePath)
        .sample(0.1)
        .show(numRows = 10, truncate = false)

      val metadataDF = spark.read.format("hudi").load(s"$basePath/.hoodie/metadata")
      metadataDF.printSchema()
      metadataDF.show(numRows = 1000, truncate = false)

      println(
        spark.read
          .format("hudi")
          .load(basePath)
          .count())

    }

  it should "not merge schemas if specified with DataFrame API" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {

      import spark.implicits._

      removeDirectory("spark-warehouse/hudi_schema")

      val format = "hudi"

      val df = Seq(1, 2).toDF("id")
      val path = s"/Users/josep/IdeaProjects/qbeast-spark/spark-warehouse/hudi_schema"
      df.write
        .format(format)
        .mode("overwrite")
        .option("hoodie.table.name", "student")
        .option("columnsToIndex", "id")
        .save(path)

      val dfExtraCol = Seq((3, "John"), (4, "Doe")).toDF("id", "name")

      // this will be executed properly
      dfExtraCol.write
        .format(format)
        .mode("append")
        .save(path)

      // This will fail
      val renamedDF = Seq((5, "John", 10)).toDF("id", "name2", "age")
      renamedDF.write
        .format(format)
        .mode("append")
        .option("columnsToIndex", "id")
        .option(SET_NULL_FOR_MISSING_COLUMNS.key, "true")
        .option("mergeSchema", "true")
        .save(path)

      //      spark.read
      //        .format("qbeast")
      //        .load(path)
      //        .schema
      //        .fieldNames shouldBe dfExtraCol.schema.fieldNames

      //      val dfExtraCol2 = Seq((3, "JohnSS"), (4, "DoeSSSS")).toDF("id", "surname")
      //
      //      // EXTRA COLUMN
      //
      //      dfExtraCol2.write
      //        .format("qbeast")
      //        .mode("append")
      //        .option("mergeSchema", "true")
      //        .option("columnsToIndex", "id")
      //        .save(path)

      spark.read
        .format(format)
        .load(path)
        .show(false)

    })

}
