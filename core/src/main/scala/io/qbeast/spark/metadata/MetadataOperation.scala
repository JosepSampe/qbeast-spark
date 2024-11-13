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
package io.qbeast.spark.metadata

import io.qbeast.core.model.mapper
import io.qbeast.core.model.Revision
import io.qbeast.core.model.StagingUtils
import io.qbeast.core.model.TableChanges
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.utils.MetadataConfig
import io.qbeast.spark.utils.MetadataConfig.lastRevisionID
import io.qbeast.spark.utils.MetadataConfig.revision

/**
 * Qbeast metadata changes on a Delta Table.
 */
trait MetadataOperation extends StagingUtils {

  type Configuration = Map[String, String]

  private def overwriteQbeastConfiguration(baseConfiguration: Configuration): Configuration = {
    val revisionKeys = baseConfiguration.keys.filter(_.startsWith(MetadataConfig.revision))
    val other = baseConfiguration.keys.filter(_ == MetadataConfig.lastRevisionID)
    val qbeastKeys = revisionKeys ++ other
    baseConfiguration -- qbeastKeys
  }

  /**
   * Update metadata with new Qbeast Revision
   * @param baseConfiguration
   *   the base configuration
   * @param newRevision
   *   the new revision
   */
  private def updateQbeastRevision(
      baseConfiguration: Configuration,
      newRevision: Revision): Configuration = {
    val newRevisionID = newRevision.revisionID

    // Add staging revision, if necessary. The qbeast metadata configuration
    // should always have a revision with RevisionID = stagingID.
    val stagingRevisionKey = s"$revision.$stagingID"
    val addStagingRevision =
      newRevisionID == 1 && !baseConfiguration.contains(stagingRevisionKey)
    val configuration =
      if (!addStagingRevision) baseConfiguration
      else {
        // Create staging revision with EmptyTransformers (and EmptyTransformations).
        // We modify its timestamp to secure loadRevisionAt
        val stagingRev =
          stagingRevision(
            newRevision.tableID,
            newRevision.desiredCubeSize,
            newRevision.columnTransformers.map(_.columnName))
            .copy(timestamp = newRevision.timestamp - 1)

        // Add the staging revision to the revisionMap without overwriting
        // the latestRevisionID
        baseConfiguration
          .updated(stagingRevisionKey, mapper.writeValueAsString(stagingRev))
      }

    // Update latest revision id and add new revision to metadata
    configuration
      .updated(lastRevisionID, newRevisionID.toString)
      .updated(s"$revision.$newRevisionID", mapper.writeValueAsString(newRevision))
  }

  /**
   * Update Qbeast Metadata
   * @param currentConfiguration
   *   Current table configuration
   * @param isNewTable
   *   whether the table is new or not
   * @param isOverwriteMode
   *   whether the write mode is overwritten
   * @param tableChanges
   *   the changes in the table
   * @param qbeastOptions
   *   the Qbeast options to update
   */

  def updateConfiguration(
      currentConfiguration: Configuration,
      isNewTable: Boolean,
      isOverwriteMode: Boolean,
      tableChanges: TableChanges,
      qbeastOptions: QbeastOptions): (Configuration, Boolean) = {

    // Either the data triggered a new revision or the user specified options to amplify the ranges
    val containsQbeastMetadata: Boolean = currentConfiguration.contains(lastRevisionID)

    // Append on an empty table
    val isNewWriteAppend = !isOverwriteMode && isNewTable
    // If the table exists, but the user added a new revision, we need to create a new revision
    val isUserUpdatedMetadata =
      containsQbeastMetadata &&
        tableChanges.updatedRevision.revisionID == currentConfiguration(lastRevisionID).toInt + 1

    // Whether:
    // 1. Data Triggered a New Revision
    // 2. User added a columnStats that triggered a new Revision
    // 3. User made an APPEND on a NEW TABLE with columnStats that triggered a new Revision
    val isNewRevision: Boolean =
      tableChanges.isNewRevision || isUserUpdatedMetadata || isNewWriteAppend

    val latestRevision = tableChanges.updatedRevision
    val baseConfiguration: Configuration =
      if (isNewTable) Map.empty
      else if (isOverwriteMode) overwriteQbeastConfiguration(currentConfiguration)
      else currentConfiguration

    // Qbeast configuration metadata
    val (qbeastConfiguration, hasRevisionUpdate) =
      if (isNewRevision || isOverwriteMode || tableChanges.isOptimizeOperation)
        (updateQbeastRevision(baseConfiguration, latestRevision), true)
      else (baseConfiguration, false)
    val qbeastExtraWriteOptions = qbeastOptions.extraOptions
    val qbeastWriteProperties = qbeastOptions.writeProperties
    // Merge the qbeast configuration with the extra configuration and the write properties
    val configuration = qbeastConfiguration ++ qbeastWriteProperties ++ qbeastExtraWriteOptions

    (configuration, hasRevisionUpdate)
  }

}
