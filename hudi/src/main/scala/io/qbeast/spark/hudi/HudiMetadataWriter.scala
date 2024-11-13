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

import com.fasterxml.jackson.core.JsonFactory
import io.qbeast.core.model._
import io.qbeast.core.model.PreCommitHook.PreCommitHookOutput
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.utils.MetadataConfig
import io.qbeast.spark.utils.TagUtils
import io.qbeast.spark.writer.StatsTracker.registerStatsTrackers
import io.qbeast.IISeq
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.model.HoodiePartitionMetadata
import org.apache.hudi.common.model.HoodieWriteStat
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.CommitUtils
import org.apache.hudi.common.util.HoodieTimer
import org.apache.hudi.common.util.Option
import org.apache.hudi.common.util.StringUtils.getUTF8Bytes
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.DataSourceUtils
import org.apache.hudi.HoodieWriterUtils
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isHoodieConfigKey
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

import java.io.StringWriter
import java.lang.System.currentTimeMillis
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
 * DeltaMetadataWriter is in charge of writing data to a table and report the necessary log
 * information
 *
 * @param tableID
 *   the table identifier
 * @param mode
 *   SaveMode of the writeMetadata
 * @param metaClient
 *   metaClient associated to the table
 * @param qbeastOptions
 *   options for writeMetadata operation
 * @param schema
 *   the schema of the table
 */
private[hudi] case class HudiMetadataWriter(
    tableID: QTableID,
    mode: SaveMode,
    metaClient: HoodieTableMetaClient,
    qbeastOptions: QbeastOptions,
    schema: StructType)
    extends HudiMetadataOperation
    with Logging {

  // private def isOverwriteMode: Boolean = mode == SaveMode.Overwrite

  private val spark = SparkSession.active
  private val jsonFactory = new JsonFactory()

  private val basePath = tableID.id
  private val logPath = new Path(basePath, ".hoodie")
  private val jsc = new JavaSparkContext(spark.sparkContext)

  private val isNewTable: Boolean = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    !fs.exists(logPath)
  }

  private def isOverwriteOperation: Boolean = mode == SaveMode.Overwrite

//  private val writeConfig = HoodieWriteConfig
//    .newBuilder()
//    .withPath(metaClient.getBasePathV2.toString)
//    .forTable(metaClient.getTableConfig.getTableName)
//    .build()

  /**
   * Creates an instance of basic stats tracker on the desired transaction
   * @return
   */
  private def createStatsTrackers(): Seq[WriteJobStatsTracker] = {
    val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()
    // Create basic stats trackers to add metrics on the Write Operation
    val hadoopConf = spark.sessionState.newHadoopConf()
    val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
      new SerializableConfiguration(hadoopConf),
      BasicWriteJobStatsTracker.metrics)
    statsTrackers.append(basicWriteJobStatsTracker)
    statsTrackers
  }

  private def createHoodieClient(): SparkRDDWriteClient[_] = {
    val avroSchema = SchemaConverters.toAvroType(schema)

    val statsTrackers = createStatsTrackers()
    registerStatsTrackers(statsTrackers)

    // If tableName is provided, we need to add catalog props
    //    val tableName = scala.Option("hudi_table")
    //    val catalogProps = tableName match {
    //      case Some(value) =>
    //        HoodieOptionConfig.mapSqlOptionsToDataSourceWriteConfigs(
    //          getHoodieCatalogTable(sparkSession, value).catalogProperties)
    //      case None => Map.empty
    //    }

    // Priority: defaults < catalog props < table config < sparkSession conf < specified conf
    val finalParameters = HoodieWriterUtils.parametersWithWriteDefaults(
      metaClient.getTableConfig.getProps.asScala.toMap ++
        spark.sqlContext.getAllConfs.filterKeys(isHoodieConfigKey) ++
        qbeastOptions.extraOptions)

    DataSourceUtils.createHoodieClient(
      jsc,
      avroSchema.toString,
      basePath,
      metaClient.getTableConfig.getTableName,
      finalParameters.asJava)
  }

  private val preCommitHooks = new ListBuffer[PreCommitHook]()

  // Load the pre-commit hooks
  loadPreCommitHooks().foreach(registerPreCommitHooks)

  /**
   * Register a pre-commit hook
   * @param preCommitHook
   *   the hook to register
   */
  private def registerPreCommitHooks(preCommitHook: PreCommitHook): Unit = {
    if (!preCommitHooks.contains(preCommitHook)) {
      preCommitHooks.append(preCommitHook)
    }
  }

  /**
   * Load the pre-commit hooks from the options
   * @return
   *   the loaded hooks
   */
  private def loadPreCommitHooks(): Seq[PreCommitHook] =
    qbeastOptions.hookInfo.map(QbeastHookLoader.loadHook)

  /**
   * Executes all registered pre-commit hooks.
   *
   * This function iterates over all pre-commit hooks registered in the `preCommitHooks`
   * ArrayBuffer. For each hook, it calls the `run` method of the hook, passing the provided
   * actions as an argument. The `run` method of a hook is expected to return a Map[String,
   * String] which represents the output of the hook. The outputs of all hooks are combined into a
   * single Map[String, String] which is returned as the result of this function.
   *
   * It's important to note that if two or more hooks return a map with the same key, the value of
   * the key in the resulting map will be the value from the last hook that returned that key.
   * This is because the `++` operation on maps in Scala is a right-biased union operation, which
   * means that if there are duplicate keys, the value from the right operand (in this case, the
   * later hook) will overwrite the value from the left operand.
   *
   * Therefore, to avoid unexpected behavior, it's crucial to ensure that the outputs of different
   * hooks have unique keys. If there's a possibility of key overlap, the hooks should be designed
   * to handle this appropriately, for example by prefixing each key with a unique identifier for
   * the hook.
   *
   * @param actions
   *   The actions to be passed to the `run` method of each hook.
   * @return
   *   A Map[String, String] representing the combined outputs of all hooks.
   */
  private def runPreCommitHooks(actions: Seq[QbeastFile]): PreCommitHookOutput = {
    preCommitHooks.foldLeft(Map.empty[String, String]) { (acc, hook) =>
      acc ++ hook.run(actions)
    }
  }

  def writeWithTransaction(
      writer: String => (TableChanges, Seq[IndexFile], Seq[DeleteFile])): Unit = {

    val hudiClient = createHoodieClient()

    val operationType = WriteOperationType.BULK_INSERT
    val commitActionType = CommitUtils.getCommitActionType(operationType, metaClient.getTableType)

    val instantTime = HoodieActiveTimeline.createNewInstantTime
    hudiClient.startCommitWithTime(instantTime, commitActionType)
    hudiClient.setOperationType(operationType)

    val hoodieTable = HoodieSparkTable.create(hudiClient.getConfig, hudiClient.getEngineContext)
    val timeLine = hoodieTable.getActiveTimeline
    val requested = new HoodieInstant(State.REQUESTED, commitActionType, instantTime)
    val metadata = new HoodieCommitMetadata
    metadata.setOperationType(operationType)
    timeLine.transitionRequestedToInflight(
      requested,
      Option.of(getUTF8Bytes(metadata.toJsonString)))

    val extraMeta = new util.HashMap[String, String]()

    val currTimer = HoodieTimer.start()
    val (tableChanges, indexFiles, deleteFiles) = writer(instantTime)
    val totalWriteTime = currTimer.endTimer()

    // Update Qbeast Metadata (replicated set, revision..)
    val qbeastFiles =
      updateMetadata(tableChanges, indexFiles, deleteFiles, qbeastOptions.extraOptions)

    val partitionMetadata =
      new HoodiePartitionMetadata(
        metaClient.getStorage,
        instantTime,
        metaClient.getBasePathV2,
        metaClient.getBasePathV2,
        Option.empty())
    partitionMetadata.trySave()

    val qbeastMetadata: mutable.Map[String, Map[String, Object]] = mutable.Map()
    val writeStatusList = ListBuffer[WriteStatus]()

    indexFiles.foreach(indexFile => {
      val writeStatus = new WriteStatus()
      writeStatus.setFileId(FSUtils.getFileId(indexFile.path))
      writeStatus.setPartitionPath("")
      writeStatus.setTotalRecords(indexFile.elementCount)

      val stat = new HoodieWriteStat()
      stat.setPartitionPath(writeStatus.getPartitionPath)
      stat.setNumWrites(writeStatus.getTotalRecords)
      stat.setNumDeletes(0)
      stat.setNumInserts(writeStatus.getTotalRecords)
      stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT)
      stat.setFileId(writeStatus.getFileId)
      stat.setPath(indexFile.path)
      stat.setTotalWriteBytes(indexFile.size)
      stat.setFileSizeInBytes(indexFile.size)
      stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords)

      val runtimeStats = new HoodieWriteStat.RuntimeStats
      runtimeStats.setTotalCreateTime(totalWriteTime)
      stat.setRuntimeStats(runtimeStats)

      writeStatus.setStat(stat)
      writeStatusList += writeStatus

      val metadata = Map(
        TagUtils.revision -> indexFile.revisionId.toString,
        TagUtils.blocks -> mapper.readTree(encodeBlocks(indexFile.blocks)))
      qbeastMetadata += (indexFile.path -> metadata)
    })

    val tags = runPreCommitHooks(qbeastFiles)

    extraMeta.put(MetadataConfig.tags, mapper.writeValueAsString(tags))
    extraMeta.put(MetadataConfig.blocks, mapper.writeValueAsString(qbeastMetadata))

    val writeStatusRdd = jsc.parallelize(writeStatusList)
    hudiClient.commit(instantTime, writeStatusRdd, Option.of(extraMeta))

  }

  def updateMetadataWithTransaction(configuration: => Configuration): Unit = {

    val hudiClient = createHoodieClient()
    val commitActionType =
      CommitUtils.getCommitActionType(WriteOperationType.ALTER_SCHEMA, metaClient.getTableType)
    val instantTime = HoodieActiveTimeline.createNewInstantTime

    hudiClient.startCommitWithTime(instantTime, commitActionType)
    hudiClient.preWrite(instantTime, WriteOperationType.ALTER_SCHEMA, metaClient)

    val hoodieTable = HoodieSparkTable.create(hudiClient.getConfig, hudiClient.getEngineContext)
    val timeLine = hoodieTable.getActiveTimeline
    val requested = new HoodieInstant(State.REQUESTED, commitActionType, instantTime)
    val metadata = new HoodieCommitMetadata
    metadata.setOperationType(WriteOperationType.ALTER_SCHEMA)
    timeLine.transitionRequestedToInflight(
      requested,
      Option.of(getUTF8Bytes(metadata.toJsonString)))

    val updatedConfig = configuration.foldLeft(getCurrentConfig) { case (accConf, (k, v)) =>
      accConf.updated(k, v)
    }

    updateTableMetadata(
      metaClient,
      schema,
      isOverwriteOperation,
      updatedConfig,
      hasRevisionUpdate = true)

    hudiClient.commit(instantTime, jsc.emptyRDD)
  }

  private def encodeBlocks(blocks: IISeq[Block]): String = {
    val writer = new StringWriter()
    val generator = jsonFactory.createGenerator(writer)
    generator.writeStartArray()
    blocks.foreach { block =>
      generator.writeStartObject()
      generator.writeStringField("cubeId", block.cubeId.string)
      generator.writeNumberField("minWeight", block.minWeight.value)
      generator.writeNumberField("maxWeight", block.maxWeight.value)
      generator.writeNumberField("elementCount", block.elementCount)
      generator.writeBooleanField("replicated", block.replicated)
      generator.writeEndObject()
    }
    generator.writeEndArray()
    generator.close()
    writer.close()
    writer.toString
  }

  /**
   * Writes metadata of the table
   *
   * @param tableChanges
   *   changes to apply
   * @param indexFiles
   *   files to add
   * @param deleteFiles
   *   files to remove
   * @param extraConfiguration
   *   extra configuration to apply
   * @return
   *   the sequence of file actions to save in the commit log(add, remove...)
   */
  protected def updateMetadata(
      tableChanges: TableChanges,
      indexFiles: Seq[IndexFile],
      deleteFiles: Seq[DeleteFile],
      extraConfiguration: Configuration): Seq[QbeastFile] = {

    if (isNewTable) {
      val fs = FileSystem.get(spark.sessionState.newHadoopConf)
      fs.mkdirs(logPath)
    } else {
      // This table already exists, check if the insert is valid.
      if (mode == SaveMode.ErrorIfExists) {
        throw AnalysisExceptionFactory.create(s"Path '${tableID.id}' already exists.'")
      } else if (mode == SaveMode.Ignore) {
        return Nil
      }
    }

    val (newConfiguration, hasRevisionUpdate) = updateConfiguration(
      getCurrentConfig,
      isNewTable,
      isOverwriteOperation,
      tableChanges,
      qbeastOptions)

    updateTableMetadata(
      metaClient,
      schema,
      isOverwriteOperation,
      newConfiguration,
      hasRevisionUpdate)

    val deletedFiles = mode match {
      case SaveMode.Overwrite =>
        indexFiles.map { indexFile =>
          DeleteFile(
            path = indexFile.path,
            size = indexFile.size,
            dataChange = false,
            deletionTimestamp = currentTimeMillis())
        }.toIndexedSeq
      case _ => deleteFiles
    }

    indexFiles ++ deletedFiles
  }

  private def getCurrentConfig: Configuration = {
    val tablePropsMap = metaClient.getTableConfig.getProps.asScala.toMap
    if (tablePropsMap.contains(MetadataConfig.configuration))
      mapper
        .readTree(tablePropsMap(MetadataConfig.configuration))
        .fields()
        .asScala
        .map(entry => entry.getKey -> entry.getValue.asText())
        .toMap
    else
      Map.empty[String, String]
  }

}
