
package org.apache.spark.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Repartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.execution._

case class MergeFiles(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val enableMerge = session.conf.get("spark.sql.merge.files.enabled", "false")
    if (enableMerge != "true") {
      return plan
    }
    logInfo("merge files rule enabled")
    val numPart = session.conf.get("spark.sql.merge.files.target", "20").toInt
    plan resolveOperators {
      case c@CreateHiveTableAsSelectCommand(_, query, _, _) if !query.isInstanceOf[Repartition] =>
        val repartition = Repartition(numPart, false, query)
        c.copy(query = repartition)
      case c@InsertIntoHiveTable(_, _, query, _, _, _) if !query.isInstanceOf[Repartition] =>
        val repartition = Repartition(numPart, false, query)
        c.copy(query = repartition)
    }
  }

}
