package io.qbeast.spark.hudi

import io.qbeast.core.model._
import io.qbeast.spark.index.IndexStatusBuilder
import io.qbeast.spark.utils.MetadataConfig
import io.qbeast.IISeq
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.exception.HoodieIOException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.HoodieFileIndex
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.execution.datasources.FileStatusCache
import org.apache.spark.sql.execution.datasources.FileStatusWithMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

case class HudiQbeastSnapshot(tableID: QTableID) extends QbeastSnapshot with StagingUtils {

  override val basePath: Path = new Path(tableID.id)

  private lazy val spark = SparkSession.active
  private lazy val jsc = new JavaSparkContext(spark.sparkContext)

  private lazy val metaClient: HoodieTableMetaClient = HoodieTableMetaClient
    .builder()
    .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
    .setBasePath(tableID.id)
    .build()

  /**
   * The current state of the snapshot.
   *
   * @return
   */
  override def isInitial: Boolean = !basePath
    .getFileSystem(jsc.hadoopConfiguration())
    .exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))

  /** Schema present in this Snapshot. */
  override lazy val schema: StructType = {
    val tableSchemaResolver = new TableSchemaResolver(metaClient)
    val avroSchema = tableSchemaResolver.getTableAvroSchemaFromLatestCommit(true)
    if (avroSchema.isPresent) {
      AvroConversionUtils.convertAvroSchemaToStructType(avroSchema.get)
    } else StructType.apply(Nil)
  }

  /** Count of all the files present in this Snapshot. */
  override lazy val numOfFiles: Long = loadFileIndex().inputFiles.length

  private lazy val metadataMap: Map[String, String] = {
    val tablePropsMap = metaClient.getTableConfig.getProps.asScala.toMap
    tablePropsMap
      .get(MetadataConfig.configuration)
      .map { configJson =>
        mapper.readValue[Map[String, String]](configJson, classOf[Map[String, String]])
      }
      .getOrElse(Map.empty)
  }

  override def loadProperties: Map[String, String] = {
    metadataMap.filterKeys(k => !k.startsWith(MetadataConfig.revision))
  }

  override def loadConfiguration: Map[String, String] = metadataMap

  override def loadDescription: String = s"Hudi table snapshot at ${tableID.id}"

  private lazy val revisionsMap: Map[RevisionID, Revision] = {
    val listRevisions = metadataMap.filterKeys(_.startsWith(MetadataConfig.revision))
    listRevisions.map { case (key, json) =>
      val revisionID = key.split('.').last.toLong
      val revision = mapper.readValue[Revision](json, classOf[Revision])
      (revisionID, revision)
    }
  }

  private lazy val lastRevisionID: RevisionID =
    metadataMap.getOrElse(MetadataConfig.lastRevisionID, "-1").toLong

  private def getRevision(revisionID: RevisionID): Revision = {
    revisionsMap.getOrElse(
      revisionID,
      throw AnalysisExceptionFactory.create(s"Revision $revisionID not found"))
  }

  override def existsRevision(revisionID: RevisionID): Boolean = revisionsMap.contains(revisionID)

  override def loadLatestIndexStatus: IndexStatus = loadIndexStatus(lastRevisionID)

  override def loadIndexStatus(revisionID: RevisionID): IndexStatus = {
    val revision = getRevision(revisionID)
    new IndexStatusBuilder(this, revision).build()
  }

  override def loadLatestIndexFiles: Dataset[IndexFile] = loadIndexFiles(lastRevisionID)

  override def loadIndexFiles(revisionID: RevisionID): Dataset[IndexFile] = {
    val dimensionCount = loadRevision(revisionID).transformations.size
    val indexFilesBuffer = ListBuffer[IndexFile]()
    val processedCommitTimes = scala.collection.mutable.Set[String]()

    val inputFiles = loadFileIndex().inputFiles

    import spark.implicits._
    if (inputFiles.isEmpty) return spark.emptyDataset[IndexFile]

    val commitTimes = inputFiles.map { filePath =>
      val fileName = new StoragePath(filePath).getName
      FSUtils.getCommitTime(fileName)
    }.distinct
    val fileNames = inputFiles.map { filePath =>
      new StoragePath(filePath).getName
    }.distinct

    def processTimeline(timeline: HoodieTimeline): Unit = {
      timeline.filterCompletedInstants.getInstants.asScala
        .filter(instant => commitTimes.contains(instant.requestedTime()))
        .foreach { instant =>
          try {
            val commitMetadataBytes = timeline.getInstantDetails(instant).get()
            val metadata = TimelineMetadataUtils.deserializeCommitMetadata(commitMetadataBytes)
            val commitMetadata =
              HoodieCommitMetadata.fromJsonString(
                metadata.toString,
                classOf[HoodieCommitMetadata])
            val indexFiles = HudiQbeastFileUtils
              .fromCommitFile(dimensionCount)(commitMetadata)
              .filter(file => file.revisionId == revisionID && fileNames.contains(file.path))
            indexFilesBuffer ++= indexFiles
            processedCommitTimes += instant.requestedTime()
          } catch {
            // This may occur if a commit obtained in the active pipeline is moved to
            // the archive pipeline during the processing of this function. In such cases,
            // we can safely ignore it, as the commit will be handled properly by the coming
            // processTimeline(archivedTimeline) call below.
            case _: HoodieIOException =>
              println(s"--> ERROR: Could not find the commit time ${instant.requestedTime()}")
          }

        }
    }
    val logFile = new File("/tmp/qbeast_hudi_execution.log")
    val writer =
      new PrintWriter(new FileWriter(logFile, true))
    writer.println("--- NEW ITERATION PROCESSED COMMITS---")

    writer.println(s"- Current snapshot commits: ${commitTimes.length}")
    val activeTimeline = metaClient.getActiveTimeline
    processTimeline(activeTimeline)
    writer.println(s"- Processed active timeline commits: ${processedCommitTimes.size}")

    if (commitTimes.diff(processedCommitTimes.toSeq).nonEmpty) {
      val archivedTimeline = metaClient.getArchivedTimeline(commitTimes.min, false)
      processTimeline(archivedTimeline)
      writer.println(
        s"- Processed active + archive timeline commits: ${processedCommitTimes.size}")
    }

    writer.println(
      s"- Active timeline commits: ${activeTimeline.filterCompletedInstants.getInstants.asScala.size}")
    val archivedTimeline = metaClient.getArchivedTimeline()
    writer.println(
      s"- Archive timeline commits: ${archivedTimeline.filterCompletedInstants.getInstants.asScala.size}")
    writer.flush()
    writer.close()

    // At this point, we verify that all Qbeast metadata is correctly loaded
    // from all the commit files associated with the Parquet files in this snapshot.
    // If there are unprocessed commit times, it indicates an inconsistency,
    // and an exception is thrown to highlight the issue.
    val unprocessedCommitTimes = commitTimes.diff(processedCommitTimes.toSeq)
    if (unprocessedCommitTimes.nonEmpty) {
      val errorMessage =
        s"Some commit times were not processed: ${unprocessedCommitTimes.mkString(", ")}"
      throw new IllegalStateException(errorMessage)
    }

    spark.createDataset(indexFilesBuffer.toList)
  }

  override def loadAllRevisions: IISeq[Revision] = revisionsMap.values.toVector

  override def loadLatestRevision: Revision = getRevision(lastRevisionID)

  override def loadRevision(revisionID: RevisionID): Revision = getRevision(revisionID)

  override def loadRevisionAt(timestamp: Long): Revision = {
    val candidateRevisions = revisionsMap.values.filter(_.timestamp <= timestamp)
    if (candidateRevisions.nonEmpty) candidateRevisions.maxBy(_.timestamp)
    else {
      throw AnalysisExceptionFactory.create(s"No space revision available before $timestamp")
    }
  }

  override def loadDataframeFromIndexFiles(indexFile: Dataset[IndexFile]): DataFrame = {
    import indexFile.sparkSession.implicits._
    val rootPath = tableID.id
    val paths = indexFile.map(ifile => new Path(rootPath, ifile.path).toString).collect()
    indexFile.sparkSession.read
      .schema(schema)
      .load(paths: _*)
  }

  /**
   * Lists the files present in the staging area
   * @param fileIndex
   *   FileIndex instance
   * @param partitionFilters
   *   Partition filters
   * @param dataFilters
   *   Data filters
   *
   * @return
   *   Sequence of FileStatusWithMetadata
   */
  override def listUnindexedFiles(
      fileIndex: FileIndex,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[FileStatusWithMetadata] = {
    val unindexedFilesDS = loadIndexFiles(stagingID)
    val getAbsolutePath = (filePath: String) => {
      val path = new Path(filePath)
      if (path.isAbsolute) path else new Path(basePath, path)
    }
    unindexedFilesDS
      .collect()
      .map { indexFile =>
        val newPath = getAbsolutePath(indexFile.path)
        FileStatusWithMetadata(
          new FileStatus(indexFile.size, false, 0, 1, indexFile.modificationTime, newPath),
          Map.empty)
      }
  }

  /**
   * Loads the file index
   * @return
   *   the FileIndex
   */
  override def loadFileIndex(): FileIndex = {
    val sparkSession = SparkSession.active
    HoodieFileIndex(
      sparkSession,
      metaClient,
      Some(schema),
      Map("path" -> tableID.id),
      FileStatusCache.getOrCreate(sparkSession))
  }

}
