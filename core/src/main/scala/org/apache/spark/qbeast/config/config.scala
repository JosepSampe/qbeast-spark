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
package org.apache.spark.qbeast

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession

package object config {

  private[config] val defaultCubeSize: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.index.defaultCubeSize")
      .version("0.2.0")
      .intConf
      .createWithDefault(5000000)

  private[config] val cubeDomainsBufferCapacity: ConfigEntry[Long] =
    ConfigBuilder("spark.qbeast.index.cubeDomainsBufferCapacity")
      .version("0.2.0")
      .longConf
      .createWithDefault(100000L)

  private[config] val defaultNumberOfRetries: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.index.numberOfRetries")
      .version("0.2.0")
      .intConf
      .createWithDefault(2)

  private[config] val columnsToIndexSelectorEnabled: ConfigEntry[Boolean] =
    ConfigBuilder("spark.qbeast.index.columnsToIndex.auto")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(false)

  private[config] val maxNumColumnsToIndex: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.index.columnsToIndex.auto.max")
      .version("0.2.0")
      .intConf
      .createWithDefault(3)

  private[config] val tableFormat: ConfigEntry[String] =
    ConfigBuilder("spark.qbeast.tableFormat")
      .version("0.8.0")
      .stringConf
      .checkValues(Set("delta"))
      .createWithDefault("delta")

  def DEFAULT_NUMBER_OF_RETRIES: Int = SparkSession.active.sparkContext.conf
    .get(defaultNumberOfRetries)

  def DEFAULT_CUBE_SIZE: Int = SparkSession.active.sparkContext.conf
    .get(defaultCubeSize)

  def DEFAULT_TABLE_FORMAT: String = SparkSession.active.sparkContext.conf
    .get(tableFormat)

  def CUBE_DOMAINS_BUFFER_CAPACITY: Long = SparkSession.active.sparkContext.conf
    .get(cubeDomainsBufferCapacity)

  def COLUMN_SELECTOR_ENABLED: Boolean =
    SparkSession.active.sparkContext.conf.get(columnsToIndexSelectorEnabled)

  def MAX_NUM_COLUMNS_TO_INDEX: Int =
    SparkSession.active.sparkContext.conf.get(maxNumColumnsToIndex)

}
