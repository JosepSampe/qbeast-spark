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
package io.qbeast.catalog

import io.qbeast.QbeastIntegrationTestSpec
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.model.DefaultHoodieRecordPayload
import org.apache.hudi.common.model.HoodieAvroPayload
import org.apache.hudi.common.model.HoodieAvroRecord
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.model.HoodieKey
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.HoodieTableMetadata
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.HoodieCLIUtils
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

// import scala.jdk.CollectionConverters._

class QbeastCatalogIntegrationTest extends QbeastIntegrationTestSpec with CatalogTestSuite {

  def createMetaClient(jsc: JavaSparkContext, basePath: String): HoodieTableMetaClient = {
    HoodieTableMetaClient.builder
      .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
      .setBasePath(basePath)
      .build
  }

  def getBasePath(sparks: SparkSession, tableName: String): String = {
    HoodieCLIUtils.getHoodieCatalogTable(sparks, tableName).tableLocation
  }

  def createHoodieRecord(
      id: String,
      name: String,
      timestamp: Long,
      schema: Schema): HoodieRecord[HoodieAvroPayload] = {
    val record: GenericRecord = new GenericData.Record(schema)
    record.put("id", id)
    record.put("name", name)
    record.put("timestamp", timestamp)
    val hoodieKey = new HoodieKey(id, "")

    val payload = new HoodieAvroPayload(Option.of(record))

    new HoodieAvroRecord(hoodieKey, payload)
  }

  "QbeastCatalog" should
    "create a table" in withTmpDir(tmpDir =>
      withExtendedSpark(sparkConf = new SparkConf()
        .setMaster("local[8]")
        .set("spark.sql.extensions", "io.qbeast.sql.HudiQbeastSparkSessionExtension")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        // .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .set("spark.sql.warehouse.dir", tmpDir)
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .set("spark.sql.catalog.qbeast_catalog", "io.qbeast.catalog.QbeastCatalog"))(sparka => {

        val data = createTestData(sparka)

        val tableName: String = "hudi_table"

        data.write.format("hudi").saveAsTable("hudi_table") // delta catalog

        val tables = sparka.sessionState.catalog.listTables("default")
        tables.size shouldBe 1
        print(tables)

        // val deltaTable = sparka.read.table("hudi_table")
        // println(deltaTable)

        val jsc = new JavaSparkContext(sparka.sparkContext)
        val basePath = getBasePath(sparka, tableName)
        val metaClient = createMetaClient(jsc, basePath)
        val metadataPath = new StoragePath(HoodieTableMetadata.getMetadataTableBasePath(basePath))

        println(metaClient.getTableConfig)
        println(metadataPath)
        println("----------")

        // Configure Hudi options
        /*        val writeConfig = HoodieWriteConfig
          .newBuilder()
          .withPath(basePath)
          .withSchema(
            "{\"type\":\"record\",\"name\":\"hudi_table_record\",\"namespace\":\"hoodie.hudi_table\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"age\",\"type\":\"int\"}]}")
          .withParallelism(2, 2)
          .withDeleteParallelism(2)
          .withStorageConfig(HoodieWriteConfig.newBuilder().build().getStorageConfig)
          .forTable(tableName)
          .build()*/

        // Step 2: Define paths and configs
        val metadataBasePath = "/path/to/hudi/table/.hoodie/metadata"
        println(metadataBasePath)

        // Step 4: Set up Hudi write config
        val writeConfig = HoodieWriteConfig
          .newBuilder()
          .withPath(basePath)
          .forTable(tableName)
          .build()

        val engineContext = new HoodieSparkEngineContext(jsc)

        // Step 5: Initialize the Metadata Writer
        val metadataWriter = SparkHoodieBackedTableMetadataWriter.create(
          HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()),
          writeConfig,
          engineContext)
        println(metadataWriter)

        // Assume commitMetadata is an existing HoodieCommitMetadata object
        val commitMetadata = new HoodieCommitMetadata()

        commitMetadata.addMetadata("new_key_1", "new_value_1")
        commitMetadata.addMetadata("new_key_2", "new_value_2")

        val extraMetadata = commitMetadata.getExtraMetadata
        extraMetadata.put("additional_key", "additional_value")
        // commitMetadata.setExtraMetadata(extraMetadata)

        // Add existing metadata (if available)
        val existingMetadata: Map[String, String] = Map(
          "schema" -> "{\"type\":\"record\",\"name\":\"hudi_table_record\",\"namespace\":\"hoodie.hudi_table\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"age\",\"type\":\"int\"}]}",
          "_hoodie.metadata.ignore.spurious.deletes" -> "true",
          "_.hoodie.allow.multi.write.on.same.instant" -> "false",
          "_hoodie.optimistic.consistency.guard.enable" -> "false")

        // Set the existing metadata
        existingMetadata.foreach { case (key, value) =>
          commitMetadata.addMetadata(key, value)
        }

        // Add new custom metadata
        commitMetadata.addMetadata("new_custom_key", "new_custom_value")

        // Serialize the commit metadata
        val serializedCommitMetadata = commitMetadata.toJsonString

        println(serializedCommitMetadata)
        println("----------")

        // Perform the commit using the HoodieWriteClient
        val commitTime = HoodieActiveTimeline.createNewInstantTime()

        // Step 2: Define the Avro schema for the records
        val schemaStr =
          """
                  |{
                  |  "type": "record",
                  |  "name": "TestRecord",
                  |  "fields": [
                  |    {"name": "id", "type": "string"},
                  |    {"name": "name", "type": "string"},
                  |    {"name": "timestamp", "type": "long"}
                  |  ]
                  |}
        """.stripMargin
        val schema = new Schema.Parser().parse(schemaStr)

        // Step 3: Create sample HoodieRecord data
        val recordsRDD =
          Seq(
            createHoodieRecord("1", "Alice", 1633035600000L, schema),
            createHoodieRecord("2", "Bob", 1633122000000L, schema))
        println(recordsRDD)

        val hoodieRecords: JavaRDD[HoodieRecord[HoodieAvroPayload]] = jsc.parallelize(
          List(
            createHoodieRecord("1", "Alice", 1633035600000L, schema),
            createHoodieRecord("2", "Bob", 1633122000000L, schema)))

        println(metadataWriter.isInitialized)

        // Create HoodieListData instance
        // val hoodieListData: HoodieListData[HoodieRecord] = HoodieListData.eager(recordsRDD)
        val hoodieTable: HoodieSparkTable[DefaultHoodieRecordPayload] =
          HoodieSparkTable.create(writeConfig, engineContext, metaClient)

        println(hoodieTable)

        val timeline = hoodieTable.getActiveTimeline

        println(timeline)

        // Create a new commit instant
        timeline.createNewInstant(
          new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime))

        // metadataWriter.update(commitMetadata, recordsRDD.asJava, commitTime)

        // Save the commit metadata as complete
        timeline.saveAsComplete(
          new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime),
          Option.of(commitMetadata.toJsonString.getBytes))
        println(metadataPath)
        println(s"Metadata commit $commitTime successful")

        val writeClient =
          new SparkRDDWriteClient[HoodieAvroPayload](engineContext, writeConfig)
        println(writeClient)

        writeClient.insertOverwrite(hoodieRecords, commitTime)

        println(commitTime)

      }))

  "QbeastCatalog" should
    "coexist with Delta table" in withTmpDir(tmpDir =>
      withExtendedSpark(sparkConf = new SparkConf()
        .setMaster("local[8]")
        .set("spark.sql.extensions", "io.qbeast.sql.HudiQbeastSparkSessionExtension")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        // .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .set("spark.sql.warehouse.dir", tmpDir)
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .set("spark.sql.catalog.qbeast_catalog", "io.qbeast.catalog.QbeastCatalog"))(spark => {

        val data = createTestData(spark)

        data.write.format("hudi").saveAsTable("hudi_table") // delta catalog

        data.write
          .format("qbeast")
          .option("columnsToIndex", "id")
          .saveAsTable("qbeast_catalog.default.qbeast_table") // qbeast catalog

        val tables = spark.sessionState.catalog.listTables("default")
        tables.size shouldBe 2

        val deltaTable = spark.read.table("hudi_table")
        val qbeastTable = spark.read.table("qbeast_catalog.default.qbeast_table")

        assertSmallDatasetEquality(
          deltaTable,
          qbeastTable,
          orderedComparison = false,
          ignoreNullable = true)

      }))

  it should
    "coexist with Delta table in the same catalog" in withQbeastContextSparkAndTmpWarehouse(
      (spark, _) => {

        val data = createTestData(spark)

        data.write.format("hudi").saveAsTable("hudi_table") // delta catalog

        data.write
          .format("qbeast")
          .option("columnsToIndex", "id")
          .saveAsTable("qbeast_table") // qbeast catalog

        val tables = spark.sessionState.catalog.listTables("default")
        tables.size shouldBe 2

        val deltaTable = spark.read.table("hudi_table")
        val qbeastTable = spark.read.table("qbeast_table")

        assertSmallDatasetEquality(
          deltaTable,
          qbeastTable,
          orderedComparison = false,
          ignoreNullable = true)

      })

  it should "crate table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    spark.sql(
      "CREATE TABLE student (id INT, name STRING, age INT) " +
        "USING qbeast OPTIONS ('columnsToIndex'='id')")

    val table = spark.table("student")
    table.schema shouldBe schema
    table.count() shouldBe 0

  })

  it should "replace table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    // Create table first (must be in qbeast format)
    spark.sql(
      "CREATE TABLE student (id INT, name STRING, age INT) " +
        "USING qbeast OPTIONS ('columnsToIndex'='age')")

    spark.sql("SHOW TABLES").count() shouldBe 1

    // Try to replace it
    spark.sql(
      "REPLACE TABLE student (id INT, name STRING, age INT) " +
        "USING qbeast OPTIONS ('columnsToIndex'='age')")

    spark.sql("SHOW TABLES").count() shouldBe 1

    val table = spark.read.table("student")
    table.schema shouldBe schema
    table.count() shouldBe 0

  })

  it should "create or replace table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    spark.sql(
      "CREATE OR REPLACE TABLE student (id INT, name STRING, age INT)" +
        " USING qbeast OPTIONS ('columnsToIndex'='id')")

    val table = spark.read.table("student")
    table.schema shouldBe schema
    table.count() shouldBe 0

  })

  it should "create table and insert data as select" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {

      spark.sql(
        "CREATE OR REPLACE TABLE student (id INT, name STRING, age INT)" +
          " USING qbeast OPTIONS ('columnsToIndex'='id')")

      import spark.implicits._
      // Create temp view with data to try SELECT AS statement
      students.toDF.createOrReplaceTempView("bronze_student")

      spark.sql("INSERT INTO table student SELECT * FROM bronze_student")
      spark.sql("SELECT * FROM student").count() shouldBe students.size

      spark.sql("INSERT INTO table student TABLE bronze_student")
      spark.sql("SELECT * FROM student").count() shouldBe students.size * 2

    })

  it should "crate external table" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {

      val tmpDir = tmpWarehouse + "/test"
      val data = createTestData(spark)
      data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

      spark.sql(
        "CREATE EXTERNAL TABLE student " +
          s"USING qbeast OPTIONS ('columnsToIndex'='id') LOCATION '$tmpDir'")

      val table = spark.table("student")
      val indexed = spark.read.format("qbeast").load(tmpDir)
      assertSmallDatasetEquality(table, indexed, orderedComparison = false)

    })

  it should "crate external table with the schema" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {

      val tmpDir = tmpWarehouse + "/test"
      val data = createTestData(spark)
      data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

      spark.sql(
        "CREATE EXTERNAL TABLE student (id INT, name STRING, age INT) " +
          s"USING qbeast OPTIONS ('columnsToIndex'='id') LOCATION '$tmpDir'")

      val table = spark.table("student")
      val indexed = spark.read.format("qbeast").load(tmpDir)
      assertSmallDatasetEquality(table, indexed, orderedComparison = false)

    })

  it should "throw error when the specified schema mismatch existing schema" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {

      val tmpDir = tmpWarehouse + "/test"
      val data = createTestData(spark)
      data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("CREATE EXTERNAL TABLE student (id INT, age INT) " +
          s"USING qbeast OPTIONS ('columnsToIndex'='id') LOCATION '$tmpDir'"))

    })

  it should "throw error when no schema and no populated table" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("CREATE EXTERNAL TABLE student " +
          s"USING qbeast OPTIONS ('columnsToIndex'='id') LOCATION '$tmpWarehouse'"))

    })

  it should "throw an error when trying to replace a non-qbeast table" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {

      spark.sql(
        "CREATE TABLE student (id INT, name STRING, age INT)" +
          " USING parquet")

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("REPLACE TABLE student (id INT, name STRING, age INT)" +
          " USING qbeast OPTIONS ('columnsToIndex'='id')"))

    })

  it should "throw an error when replacing non-existing table" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("REPLACE TABLE student (id INT, name STRING, age INT)" +
          " USING qbeast OPTIONS ('columnsToIndex'='id')"))

    })

  it should "throw an error when using partitioning/bucketing" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("CREATE OR REPLACE TABLE student (id INT, name STRING, age INT)" +
          " USING qbeast OPTIONS ('columnsToIndex'='id') PARTITIONED BY (id)"))

    })

  it should "persist altered properties on the _delta_log" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpDir) => {

      spark.sql("CREATE TABLE t1(id INT) USING qbeast TBLPROPERTIES ('columnsToIndex'= 'id')")
      spark.sql("ALTER TABLE t1 SET TBLPROPERTIES ('k' = 'v')")

      // Check the delta log info
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t1"))
      val snapshot = deltaLog.update()
      val properties = snapshot.getProperties

      properties should contain key "k"
      properties("k") shouldBe "v"

    })

  it should "persist UNSET properties in _delta_catalog" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {

      spark.sql(
        "CREATE TABLE t1(id INT) " +
          "USING qbeast " +
          "TBLPROPERTIES ('columnsToIndex'= 'id')")

      spark.sql("ALTER TABLE t1 SET TBLPROPERTIES ('k' = 'v')")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t1"))
      val snapshot = deltaLog.update()
      val properties = snapshot.getProperties

      properties should contain key "k"
      properties("k") shouldBe "v"

      spark.sql("ALTER TABLE t1 UNSET TBLPROPERTIES ('k')")

      // Check the delta log info
      val updatedProperties = deltaLog.update().getProperties
      updatedProperties should not contain key("k")
    })

  it should "ensure consistency with the session catalog" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpDir) => {

      import spark.implicits._

      spark.sql(
        "CREATE TABLE t1(id INT) " +
          "USING qbeast " +
          "TBLPROPERTIES ('columnsToIndex'= 'id')")
      spark.sql("ALTER TABLE t1 SET TBLPROPERTIES ('k' = 'v')")
      // Check the delta log info
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t1"))
      val catalog = spark.sessionState.catalog
      val showProperties = spark.sql("SHOW TBLPROPERTIES t1").as[(String, String)].collect().toMap

      val snapshot = deltaLog.update()
      val properties = snapshot.getProperties
      val catalogProperties =
        catalog.getTableMetadata(TableIdentifier("t1")).properties
      properties should contain key "k"
      catalogProperties should contain key "k"
      showProperties should contain key "k"

      spark.sql("ALTER TABLE t1 UNSET TBLPROPERTIES ('k')")
      // Check the delta log info
      val updatedSnapshot = deltaLog.update()
      val updatedProperties = updatedSnapshot.getProperties
      val updatedCatalogProperties =
        catalog.getTableMetadata(TableIdentifier("t1")).properties
      val updatedShowProperties =
        spark.sql("SHOW TBLPROPERTIES t1").as[(String, String)].collect().toMap

      updatedProperties should not contain key("k")
      updatedCatalogProperties should not contain key("k")
      updatedShowProperties should not contain key("k")
    })

  it should "persist ALL original properties of table" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpDir) => {

      spark.sql(
        s"CREATE TABLE t1(id INT) USING qbeast LOCATION '$tmpDir' " +
          "TBLPROPERTIES('k' = 'v', 'columnsToIndex' = 'id')")

      // Check the delta log info
      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val snapshot = deltaLog.update()
      val properties = snapshot.getProperties

      properties should contain key "columnsToIndex"
      properties should contain key "k"
      properties("columnsToIndex") shouldBe "id"
      properties("k") shouldBe "v"

    })

}
