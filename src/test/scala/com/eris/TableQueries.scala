package com.eris


import com.datastax.driver.core.{Row, Session}
import org.joda.time.DateTime
import scala.collection.JavaConverters._

trait TableQueries {

  def getRowsFromTable(table: String)(implicit session:Session): List[Row] = {
    session.execute(s"select * from $table").all().asScala.toList
  }

  def getBooleanFieldFromStorage(dag: DAG, fieldName: String)(implicit session:Session): Boolean = getFieldFromStorage(dag, fieldName).getBool(0)

  def getStringFieldFromStorage(dag: DAG, fieldName: String)(implicit session:Session): String = getFieldFromStorage(dag, fieldName).getString(0)

  private def getFieldFromStorage(dag: DAG, fieldName: String)(implicit session:Session): Row = {
    val query: String = s"select $fieldName from workflows where clientId = ${dag.clientId} and workflowId = ${dag.dagId} and versionId = ${dag.versionId}"
    session.execute(query).one()
  }

  def getDAGFromStorage(dagId: String, versionId: String)(implicit session:Session): DAGResponse = {
    val query: String = s"select * from workflows where workflowId = $dagId and versionId = $versionId allow filtering"
    val row = session.execute(query).one()
    import argonaut._, Argonaut._
    import scala.collection.JavaConversions._

    val dId = row.getUUID("workflowId").toString
    val vId = row.getUUID("versionId").toString
    val description = row.getString("description").toString
    val parentVersionId = Option(row.getUUID("parentVersionId")).map(_.toString)
    val clientId = row.getUUID("clientId").toString
    val name = row.getString("name")
    val author = row.getString("author")
    val available = row.getBool("available")
    val domain = row.getString("domain")
    val invocationParams = row.getString("invocationParams").parseOption.get
    val environment = row.getSet("environments", classOf[String]).toList
    val timestamp = new DateTime(row.getDate("timestamp"))
    val graphJson = row.getString("graphJson").parseOption.get
    DAGResponse(dId,
      vId,
      parentVersionId,
      clientId,
      name,
      description,
      author,
      available,
      domain,
      invocationParams,
      environment.toList,
      timestamp,
      graphJson)
  }
}
