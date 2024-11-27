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

import io.qbeast.core.model._
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.IISeq
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.model.HoodieTimelineTimeZone
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths
import scala.jdk.CollectionConverters.mapAsJavaMapConverter

/**
 * Spark+Delta implementation of the MetadataManager interface
 */
object HudiMetadataManager extends MetadataManager {

  private val jsc = new JavaSparkContext(SparkSession.active.sparkContext)
  private val spark = SparkSession.active
  private val hadoopConf = spark.sparkContext.hadoopConfiguration

  override def updateWithTransaction(
      tableID: QTableID,
      schema: StructType,
      options: QbeastOptions,
      mode: String)(
      writer: String => (TableChanges, IISeq[IndexFile], IISeq[DeleteFile])): Unit = {

    if (!existsLog(tableID)) createTable(tableID, options.extraOptions)

    val tableSchema = loadCurrentSchema(tableID)
    val metaClient = loadMetaClient(tableID)
    val metadataWriter =
      HudiMetadataWriter(tableID, mode, metaClient, options, tableSchema, schema)

    metadataWriter.writeWithTransaction(writer)
  }

  override def updateMetadataWithTransaction(tableID: QTableID, dataSchema: StructType)(
      config: => Configuration): Unit = {

    val tableSchema = loadCurrentSchema(tableID)
    val metaClient = loadMetaClient(tableID)
    val metadataWriter =
      HudiMetadataWriter(
        tableID,
        SaveMode.Append.toString,
        metaClient,
        QbeastOptions.empty,
        tableSchema,
        dataSchema)

    metadataWriter.updateMetadataWithTransaction(config)

  }

  override def overwriteMetadataWithTransaction(tableID: QTableID, dataSchema: StructType)(
      config: => Configuration): Unit = {

    val tableSchema = loadCurrentSchema(tableID)
    val metaClient = loadMetaClient(tableID)
    val metadataWriter =
      HudiMetadataWriter(
        tableID,
        SaveMode.Append.toString,
        metaClient,
        QbeastOptions.empty,
        tableSchema,
        dataSchema)

    metadataWriter.overwriteMetadataWithTransaction(config)

  }

  override def loadSnapshot(tableID: QTableID): HudiQbeastSnapshot = {
    HudiQbeastSnapshot(tableID)
  }

  override def loadCurrentSchema(tableID: QTableID): StructType = {
    loadSnapshot(tableID).schema
  }

  override def updateRevision(tableID: QTableID, revisionChange: RevisionChange): Unit = {}

  override def updateTable(tableID: QTableID, tableChanges: TableChanges): Unit = {}

  /**
   * Returns the MetaClient for the table
   * @param tableID
   *   the table ID
   * @return
   */
  private def loadMetaClient(tableID: QTableID): HoodieTableMetaClient = {
    HoodieTableMetaClient
      .builder()
      .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
      .setBasePath(tableID.id)
      .build()
  }

  override def hasConflicts(
      tableID: QTableID,
      revisionID: RevisionID,
      knownAnnounced: Set[CubeId],
      oldReplicatedSet: ReplicatedSet): Boolean = {

    val snapshot = loadSnapshot(tableID)
    if (snapshot.isInitial) return false

    val newReplicatedSet = snapshot.loadIndexStatus(revisionID).replicatedSet
    val deltaReplicatedSet = newReplicatedSet -- oldReplicatedSet
    val diff = deltaReplicatedSet -- knownAnnounced
    diff.nonEmpty
  }

  /**
   * Checks if there's an existing log directory for the table
   *
   * @param tableID
   *   the table ID
   * @return
   */
  override def existsLog(tableID: QTableID): Boolean = {
    val hoodiePath = new Path(tableID.id, HoodieTableMetaClient.METAFOLDER_NAME)
    FileSystem.get(hadoopConf).exists(hoodiePath)
  }

  /**
   * Creates an initial log directory
   *
   * @param tableID
   *   Table ID
   */
  override def createLog(tableID: QTableID): Unit = {
    if (!existsLog(tableID)) {
      val hoodiePath = new Path(tableID.id, HoodieTableMetaClient.METAFOLDER_NAME)
      FileSystem.get(hadoopConf).mkdirs(hoodiePath)
    }
  }

  /**
   * Creates the initial table files
   *
   * @param tableID
   *   Table ID
   */
  private def createTable(tableID: QTableID, tableConfigs: Map[String, String]): Unit = {
    val storageConfig = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration())
    val properties = TypedProperties.fromMap(tableConfigs.asJava)
    // val hoodieConfig = HoodieWriterUtils.parametersWithWriteDefaults(tableConfigs)

    HoodieTableMetaClient
      .withPropertyBuilder()
      .setTableType(HoodieTableType.COPY_ON_WRITE)
      .setBaseFileFormat(HoodieFileFormat.PARQUET.name())
      .setCommitTimezone(HoodieTimelineTimeZone.LOCAL)
      .setHiveStylePartitioningEnable(false)
      .setPartitionMetafileUseBaseFormat(false)
      .setCDCEnabled(false)
      .setPopulateMetaFields(true)
      .setUrlEncodePartitioning(false)
      .setKeyGeneratorClassProp("org.apache.hudi.keygen.NonpartitionedKeyGenerator")
      .setDatabaseName("")
      .setPartitionFields("")
      .setTableName(Paths.get(tableID.id).getFileName.toString)
      .fromProperties(properties)
      .initTable(storageConfig, tableID.id)
  }

}
