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
package io.qbeast.sql

import io.delta.sql.DeltaSparkSessionExtension
import io.qbeast.internal.rules.QbeastAnalysis
import io.qbeast.internal.rules.SampleRule
import io.qbeast.internal.rules.SaveAsTableRule
import org.apache.spark.sql.SparkSessionExtensions

/**
 * Qbeast rules extension to spark query analyzer/optimizer/planner
 */
class QbeastSparkSessionExtension extends DeltaSparkSessionExtension {

  override def apply(extensions: SparkSessionExtensions): Unit = {

    super.apply(extensions)

    extensions.injectResolutionRule { session =>
      new QbeastAnalysis(session)
    }

    extensions.injectOptimizerRule { session =>
      new SampleRule(session)
    }

    extensions.injectOptimizerRule { session =>
      new SaveAsTableRule(session)
    }
  }

}
