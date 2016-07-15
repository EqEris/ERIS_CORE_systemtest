package com.eris

import java.nio.file.{Files, Paths}
import java.util.{Calendar, Date, UUID}

import argonaut.Json
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core._
import com.eris.Config
import com.google.common.base.Strings
import com.google.common.util.concurrent.{FutureCallback, Futures}
import org.apache.commons.codec.binary.Base64
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest._

import scala.concurrent.Promise
import scala.util.Try
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.Source


object JavaScalaConversion {
  //if we choose to use the `...async` methods
  implicit class ScalaFuture(val f: ResultSetFuture) extends AnyVal {
    def asScalaFuture = {
      val p = Promise[ResultSet]()
      Futures.addCallback(f,
        new FutureCallback[ResultSet] {
          def onSuccess(r: ResultSet) = p success r
          def onFailure(t: Throwable) = p failure t
        })
      p.future
    }
  }

  implicit class insertValues(val insert:Insert) extends AnyVal {
    def values(vals:(String,Any)*) = {
      vals.foldLeft(insert)((i, v) => i.value(v._1, v._2))
    }
  }


}

trait DBUtils extends Config with Matchers with BeforeAndAfter with BeforeAndAfterAll { this: Suite =>

  before {
    clearTable
  }

  val cluster = Cluster.builder().addContactPoint(cassandraHost).withPort(cassandraPort).build
  implicit val  session: Session = cluster.connect("dag")
  val sessionRollingMetrics = cluster.connect("rolling_metrics")
  val sessionAdwords = cluster.connect("adwords")
  val sessionConfig = cluster.connect("config")
  val sessionAudit = cluster.connect("auditservice")
  val sessionProfiling = cluster.connect("profilingservice")
  val sessionClientServicesIAM = cluster.connect("clientservicesiam")

  override def afterAll() {
    session.close()
    sessionRollingMetrics.close()
    sessionAdwords.close()
    sessionConfig.close()
    sessionAudit.close()
    sessionProfiling.close()
    sessionClientServicesIAM.close()
    cluster.close()
  }

  def countAllFromStorage: Long = {
    session.execute("select count(*) from workflows").iterator().next().getLong(0)
  }

  def clearTable: Unit = {
    val tables = Seq("workflows", "environmentVersions", "latest", "default")
    tables foreach { table => session.execute(s"truncate $table") }
    Try {
      assert(countAllFromStorage == 0)
    }.recover {
      case t: Throwable => {
        //hack :(
        Thread.sleep(1000)
        tables foreach { table => session.execute(s"truncate $table") }
      }
    }

    val rollingMetricsTables = Seq("rolledupdagnodemetrics", "rolledupdagversionmetrics", "rolledupdagversionfunctionmetrics", "rolledupclientmetrics", "rolledupnodemetrics", "rawnodemetrics", "rolledupdagfunctionmetrics", "rawdagmetrics", "rolledupmetadata", "rolledupdagversionnodemetrics", "rawloggingrequest", "rolledupfunctionmetrics", "rolledupdagmetrics")
    rollingMetricsTables foreach { table => sessionRollingMetrics.execute(s"truncate $table") }
    val adwordsTables = Seq("placementsperformance", "urls", "adgroups", "urlsources")
    adwordsTables foreach { table => sessionAdwords.execute(s"truncate $table") }
    val configTables = Seq("keywords", "backlinks")
    configTables foreach { table => sessionConfig.execute(s"truncate $table") }
    val auditTables = Seq("placementsaudit")
    auditTables foreach { table => sessionAudit.execute(s"truncate $table") }
    val profilingTables = Seq("blueconic", "usersession", "usereventtimes", "usereventcounts", "iplocations", "erisprofile")
    profilingTables foreach { table => sessionProfiling.execute(s"truncate $table") }
  }
}
