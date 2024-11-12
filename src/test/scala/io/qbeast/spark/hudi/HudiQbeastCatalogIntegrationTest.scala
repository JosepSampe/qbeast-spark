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

import io.qbeast.QbeastIntegrationTestSpec
import org.apache.avro.generic.GenericData
import org.apache.avro.Schema
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.model.DefaultHoodieRecordPayload
import org.apache.hudi.common.model.HoodieAvroPayload
import org.apache.hudi.common.model.HoodieAvroRecord
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.model.HoodieKey
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.table.timeline.HoodieInstant
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
import org.apache.hudi.HoodieCLIUtils
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import java.io.File
import java.nio.file.Paths
import java.util
import scala.collection.JavaConverters._
import scala.reflect.io.Directory
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

  val students: Seq[Student] = {
    1.to(1000).map { i =>
      val name = names(Random.nextInt(names.length)) // Randomly select a name from the list
      val age = Random.nextInt(30) + 18 // Random age between 18 and 47
      Student(i, name, age)
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

  def createTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    StudentGenerator.students.toDF()
  }

  def removeDirectory(directoryPath: String): Unit = {
    val directory = new File(directoryPath)

    if (directory.exists() && directory.isDirectory) {
      val d = new Directory(directory)
      d.deleteRecursively()
      println(s"Deleted directory: $directoryPath")
    } else {
      println(s"Directory does not exist: $directoryPath")
    }
  }

  def createHoodieRecordsFromDataFrame(
      df: DataFrame,
      schema: Schema): JavaRDD[HoodieRecord[HoodieAvroPayload]] = {
    df.rdd.map(row => HudiUtils.createHoodieRecordFromRow(row, schema)).toJavaRDD()
  }

  val hudiSparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .set("spark.sql.extensions", "io.qbeast.sql.HudiQbeastSparkSessionExtension")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .set("hoodie.metadata.enable", "true")
    .set("hoodie.file.index.enable", "true")
    .set("spark.qbeast.tableFormat", "hudi")
    .set("spark.qbeast.index.defaultCubeSize", "100")

  // .set("spark.sql.catalog.qbeast_catalog", "io.qbeast.catalog.QbeastCatalog")

  "Hudi Qbeast Catalog" should
    "create a table" in withExtendedSparkAndTmpDir(hudiSparkConf) { (spark, tmpDir) =>
      val data = createTestData(spark)
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

        val data = createTestData(spark)

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
        val commitTime = HoodieActiveTimeline.createNewInstantTime
        val hoodieInstant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime)
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

      val data = createTestData(spark)

      data.write
        .format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.table.type", "COPY_ON_WRITE")
        .option("hoodie.metadata.enable", "true")
        .option("hoodie.metadata.index.column.stats.enable", "true")
        .option("hoodie.metadata.index.bloom.filter.enable", "true")
        .option("hoodie.metadata.record.index.enable", "true")
        .saveAsTable(tableName)

      val hudiTable = spark.read.table(tableName)
      hudiTable.printSchema()

      val avroSchema = SchemaConverters.toAvroType(hudiTable.schema)
      val writeSchema = HoodieAvroUtils.removeMetadataFields(avroSchema)
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
      val instantTime = HoodieActiveTimeline.createNewInstantTime

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

        val data = createTestData(spark)

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
        val instantTime = HoodieActiveTimeline.createNewInstantTime
        println(instantTime)

        // val metadataWriter = hoodieTable.getMetadataWriter(instantTime).get()

        // println(metadataWriter)
        // println(metadataWriter.isInitialized)

        val bloomFiltersList: java.util.List[MetadataPartitionType] =
          new util.ArrayList[MetadataPartitionType]()
        bloomFiltersList.add(MetadataPartitionType.RECORD_INDEX)

        println(metaClient.getTableConfig.getMetadataPartitions)
        metadataWriter.dropMetadataPartitions(bloomFiltersList)
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
      val tableName: String = "hudi_table"
      val currentPath = Paths.get("").toAbsolutePath.toString
      val basePath = s"$currentPath/spark-warehouse/$tableName"

      removeDirectory(basePath)

      val data = createTestData(spark)

      data.write
        .format("qbeast")
        .option("hoodie.table.name", tableName)
        .option("columnsToIndex", "id")
        .option("hoodie.metadata.enable", "true")
        .option("hoodie.metadata.index.column.stats.enable", "true")
        // .option("hoodie.metadata.index.bloom.filter.enable", "true")
        // .option("hoodie.metadata.record.index.enable", "true")
        .save(basePath)

      Thread.sleep(2000)
      val data2 = createTestData(spark)
      data2.write
        .format("hudi")
        .mode("append")
        .save(basePath)
    }

  it should
    "read qbeast hudi files" in withExtendedSparkAndTmpDir(hudiSparkConf) { (spark, tmpDir) =>
      val tableName: String = "hudi_table"
      val currentPath = Paths.get("").toAbsolutePath.toString
      val basePath = s"$currentPath/spark-warehouse/$tableName"

      val metadataDF = spark.read.format("hudi").load(s"$basePath/.hoodie/metadata")
      metadataDF.printSchema()
      metadataDF.show(numRows = 100, truncate = false)

      spark.read
        .format("qbeast")
        .load(basePath)
        .sample(0.1)
        .show(numRows = 10, truncate = false)

      println(
        spark.read
          .format("qbeast")
          .load(basePath)
          .count())

    }

}
