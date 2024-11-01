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
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

/**
 * Spark+Delta implementation of the MetadataManager interface
 */
object HudiMetadataManager extends MetadataManager {

  override def updateWithTransaction(
      tableID: QTableID,
      schema: StructType,
      options: QbeastOptions,
      append: Boolean)(writer: => (TableChanges, IISeq[IndexFile], IISeq[DeleteFile])): Unit = {

    val mode = if (append) SaveMode.Append else SaveMode.Overwrite

    val metaClient = loadMetaClient(tableID)

    val metadataWriter =
      HudiMetadataWriter(tableID, mode, metaClient, options, schema)

    metadataWriter.writeWithTransaction(writer)
  }

  override def updateMetadataWithTransaction(tableID: QTableID, schema: StructType)(
      update: => Configuration): Unit = {

    val metaClient = loadMetaClient(tableID)
    val metadataWriter =
      HudiMetadataWriter(tableID, mode = SaveMode.Append, metaClient, QbeastOptions.empty, schema)

    metadataWriter.updateMetadataWithTransaction(update)

  }

  override def loadSnapshot(tableID: QTableID): HudiQbeastSnapshot = {
    // Check a better way to create the table if it does not exist
    createLog(tableID)
    HudiQbeastSnapshot(tableID)
  }

  override def loadCurrentSchema(tableID: QTableID): StructType = {
    val metaClient = loadMetaClient(tableID)
    val timeline = metaClient.getActiveTimeline.getCommitTimeline.filterCompletedInstants
    val lastCommitInstant = timeline.lastInstant().get()
    val commitMetadataBytes = metaClient.getActiveTimeline
      .getInstantDetails(lastCommitInstant)
      .get()
    val commitMetadata =
      HoodieCommitMetadata.fromBytes(commitMetadataBytes, classOf[HoodieCommitMetadata])
    val extraMetadata = commitMetadata.getExtraMetadata.get("schema")

    StructType.fromDDL(extraMetadata)
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
    val jsc = new JavaSparkContext(SparkSession.active.sparkContext)
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
    val spark = SparkSession.active
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    // val hadoopConf = spark.sessionState.newHadoopConf()
    val fs = FileSystem.get(hadoopConf)
    val hoodiePath = new Path(tableID.id, ".hoodie")
    fs.exists(hoodiePath)
  }

  /**
   * Creates an initial log directory
   *
   * @param tableID
   *   Table ID
   */
  override def createLog(tableID: QTableID): Unit = {
    val jsc = new JavaSparkContext(SparkSession.active.sparkContext)
    HoodieTableMetaClient
      .withPropertyBuilder()
      .setTableType(HoodieTableType.COPY_ON_WRITE)
      .setTableName("hudi_table")
      .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), tableID.id)
  }

}
