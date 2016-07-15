package com.eris

import java.nio.file.{Files, Paths}
import java.util.UUID

import com.datastax.driver.core.Session
import org.apache.commons.codec.binary.Base64
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}


trait InsertData {

  def insertHeatmapMetricsIntoDB(dag: DAG)(implicit session:Session) = {

    def currentTimeRoundHour = {
      val now = new DateTime(DateTimeZone.UTC)
      now.hourOfDay().roundFloorCopy().getMillis()
    }

    val rounded = currentTimeRoundHour
    val hour = rounded - 3600000
    def hours(a: Long) = (3600000L * a)
    val twelveHour = rounded - hours(12)
    val day = rounded - hours(24)
    val week = rounded - hours(168)
    val month = rounded - hours(720)
    val year = rounded - hours(8760)


    val query = {
      s"""
              BEGIN BATCH
              INSERT INTO metrics.dagrollups (clientId, dagId, timeUnit, environment, time, hits)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $hour, 5);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $hour , 'start', 5, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $hour , 'end', 5, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $hour , 'AddAndResultInT', 5, 6, 15, 50);
              INSERT INTO metrics.dagrollups (clientId, dagId, timeUnit, environment, time, hits)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $twelveHour, 10);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $twelveHour , 'start', 10, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $twelveHour , 'end', 10, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $twelveHour , 'AddAndResultInT', 10, 6, 15, 50);
              INSERT INTO metrics.dagrollups (clientId, dagId, timeUnit, environment, time, hits)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $day, 25);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $day , 'start', 25, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $day , 'end', 25, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $day , 'AddAndResultInT', 25, 6, 15, 50);
              INSERT INTO metrics.dagrollups (clientId, dagId, timeUnit, environment, time, hits)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $week, 100);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $week , 'start', 100, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $week , 'end', 100, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $week , 'AddAndResultInT', 100, 6, 15, 50);
              INSERT INTO metrics.dagrollups (clientId, dagId, timeUnit, environment, time, hits)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $month, 300);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $month , 'start', 300, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $month , 'end', 300, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $month , 'AddAndResultInT', 300, 6, 15, 50);
              INSERT INTO metrics.dagrollups (clientId, dagId, timeUnit, environment, time, hits)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $year, 3000);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $year , 'start', 3000, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $year , 'end', 3000, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time , nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $year , 'AddAndResultInT', 3000, 6, 15, 50);
              APPLY BATCH;"""
    }
    val query2 = s"""
              BEGIN BATCH
              INSERT INTO metrics.dagversionrollups (clientId, dagId, timeUnit, environment, time, dagVersionId, hits)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $hour, ${dag.versionId}, 5);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $hour,  ${dag.versionId}, 'start', 5, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $hour,  ${dag.versionId}, 'end', 5, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $hour,  ${dag.versionId}, 'AddAndResultInT', 5, 6, 15, 50);
              INSERT INTO metrics.dagversionrollups (clientId, dagId, timeUnit, environment, time, dagVersionId, hits)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $twelveHour, ${dag.versionId}, 10);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $twelveHour, ${dag.versionId}, 'start', 10, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time , dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $twelveHour, ${dag.versionId}, 'end', 10, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $twelveHour, ${dag.versionId}, 'AddAndResultInT', 10, 6, 15, 50);
              INSERT INTO metrics.dagversionrollups (clientId, dagId, timeUnit, environment, time, dagVersionId, hits)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $day, ${dag.versionId}, 25);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $day, ${dag.versionId}, 'start', 25, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $day, ${dag.versionId}, 'end', 25, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $day, ${dag.versionId}, 'AddAndResultInT', 25, 6, 15, 50);
              INSERT INTO metrics.dagversionrollups (clientId, dagId, timeUnit, environment, time, dagVersionId, hits)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $week, ${dag.versionId}, 100);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $week, ${dag.versionId}, 'start', 100, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $week, ${dag.versionId}, 'end', 100, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $week, ${dag.versionId}, 'AddAndResultInT', 100, 6, 15, 50);
              INSERT INTO metrics.dagversionrollups (clientId, dagId, timeUnit, environment, time, dagVersionId, hits)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $month, ${dag.versionId}, 300);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $month, ${dag.versionId}, 'start', 300, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $month, ${dag.versionId}, 'end', 300, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $month, ${dag.versionId}, 'AddAndResultInT', 300, 6, 15, 50);
              INSERT INTO metrics.dagversionrollups (clientId, dagId, timeUnit, environment, time, dagVersionId, hits)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $year, ${dag.versionId}, 3000);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $year, ${dag.versionId},'start', 3000, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $year, ${dag.versionId}, 'end', 3000, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${dag.clientId}, ${dag.dagId}, 'PT1H', 'test', $year, ${dag.versionId}, 'AddAndResultInT', 3000, 6, 15, 50);
              APPLY BATCH;"""
    session.execute(query)
    session.execute(query2)
  }

  def insertHeatmapMetricsVersionsIntoDB(version1: DAG, version2: DAG)(implicit session:Session) = {

    def currentTimeRoundHour = {
      val now = new DateTime(DateTimeZone.UTC)
      now.hourOfDay().roundFloorCopy().getMillis()
    }

    val rounded = currentTimeRoundHour
    val hour = rounded - 3600000

    val query = s"""
              BEGIN BATCH
              INSERT INTO metrics.dagrollups (clientId, dagId, timeUnit, environment, time, hits)
              VALUES(${version1.clientId}, ${version1.dagId}, 'PT1H', 'test', $hour, 15);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${version1.clientId}, ${version1.dagId}, 'PT1H', 'test', $hour, 'start', 15, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${version1.clientId}, ${version1.dagId}, 'PT1H', 'test', $hour, 'end', 15, 6, 15, 50);
              INSERT INTO metrics.dagnoderollups (clientId, dagId, timeUnit, environment, time, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${version1.clientId}, ${version1.dagId}, 'PT1H', 'test', $hour, 'AddAndResultInT', 15, 6, 15, 50);
              INSERT INTO metrics.dagversionrollups (clientId, dagId, timeUnit, environment, time, dagVersionId, hits)
              VALUES(${version1.clientId}, ${version1.dagId}, 'PT1H', 'test', $hour, ${version1.versionId}, 5);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${version1.clientId}, ${version1.dagId}, 'PT1H', 'test', $hour, ${version1.versionId}, 'start', 5, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${version1.clientId}, ${version1.dagId}, 'PT1H', 'test', $hour, ${version1.versionId}, 'end', 5, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${version1.clientId}, ${version1.dagId}, 'PT1H', 'test', $hour, ${version1.versionId}, 'AddAndResultInT', 5, 6, 15, 50);
              INSERT INTO metrics.dagversionrollups (clientId, dagId, timeUnit, environment, time, dagVersionId, hits)
              VALUES(${version2.clientId}, ${version2.dagId}, 'PT1H', 'test', $hour, ${version2.versionId}, 10);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${version2.clientId}, ${version2.dagId}, 'PT1H', 'test', $hour, ${version2.versionId}, 'start', 10, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${version2.clientId}, ${version2.dagId}, 'PT1H', 'test', $hour, ${version2.versionId}, 'end', 10, 6, 15, 50);
              INSERT INTO metrics.dagversionnoderollups (clientId, dagId, timeUnit, environment, time, dagVersionId, nodeId, numberOfExecutions, minExecutionTime, maxExecutionTime, totalExecutionTime)
              VALUES(${version2.clientId}, ${version2.dagId}, 'PT1H', 'test', $hour, ${version2.versionId}, 'AddAndResultInT', 10, 6, 15, 50);
              APPLY BATCH;"""
    session.execute(query)
  }

  def insertRawMetricsDataIntoDB(dag: DAG)(implicit session:Session) = {

    val query = s"""
                   BEGIN BATCH
                   INSERT INTO metrics.rawdagmetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, requestid, executiontimemillis)
                   VALUES(1443398400000, 'test', 1443444240000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId},'b0a383f1-8408-7c3f-33cb-71a8d0ace0b2', 30);
                   INSERT INTO metrics.rawdagmetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, requestid, executiontimemillis)
                   VALUES(1443398400000, 'test', 1443444240000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId},'b0a383f1-8408-7c3f-33cb-71a8d0ace0b3', 30);
                   INSERT INTO metrics.rawdagmetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, requestid, executiontimemillis)
                   VALUES(1443916800000, 'test', 1443957420000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId},'b0a383f1-8408-7c3f-33cb-71a8d0ace0b4', 30);
                   INSERT INTO metrics.rawdagmetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, requestid, executiontimemillis)
                   VALUES(1443916800000, 'test', 1443959220000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId},'b0a383f1-8408-7c3f-33cb-71a8d0ace0b5', 30);
                   INSERT INTO metrics.rawdagmetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, requestid, executiontimemillis)
                   VALUES(1443916800000, 'test', 1443959220000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId},'b0a383f1-8408-7c3f-33cb-71a8d0ace0b6', 30);
                   INSERT INTO metrics.rawdagmetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, requestid, executiontimemillis)
                   VALUES(1443916800000, 'test', 1443961020000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId},'b0a383f1-8408-7c3f-33cb-71a8d0ace0b7', 30);
                   INSERT INTO metrics.rawdagmetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, requestid, executiontimemillis)
                   VALUES(1443916800000, 'test', 1443961020000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId},'b0a383f1-8408-7c3f-33cb-71a8d0ace0b8', 30);
                   INSERT INTO metrics.rawdagmetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, requestid, executiontimemillis)
                   VALUES(1443916800000, 'test', 1443962820000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId},'b0a383f1-8408-7c3f-33cb-71a8d0ace0b9', 30);
                   INSERT INTO metrics.rawdagmetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, requestid, executiontimemillis)
                   VALUES(1443916800000, 'test', 1443962820000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId},'b0a383f1-8408-7c3f-33cb-71a8d0ace0b10', 30);
                   INSERT INTO metrics.rawdagmetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, requestid, executiontimemillis)
                   VALUES(1443916800000, 'test', 1443962820000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId},'b0a383f1-8408-7c3f-33cb-71a8d0ace0b11', 30);
                   APPLY BATCH;"""

    val query2 = s"""
                    BEGIN BATCH
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443398400000, 'test', 1443444240000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'start', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b2', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443398400000, 'test', 1443444240000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'start', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b3', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443957420000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'start', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b4', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443959220000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'start', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b5', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443959220000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'start', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b6', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443961020000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'start', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b7', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443961020000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'start', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b8', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443962820000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'start', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b9', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443962820000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'start', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b10', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443962820000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'start', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b11', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443398400000, 'test', 1443444240000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'test', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b2', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443398400000, 'test', 1443444240000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'test', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b3', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443957420000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'test', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b4', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443959220000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'test', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b5', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443959220000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'test', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b6', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443961020000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'test', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b7', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443961020000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'test', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b8', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443962820000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'test', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b9', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443962820000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'test', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b10', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443962820000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'test', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b11', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443398400000, 'test', 1443444240000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'end', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b2', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443398400000, 'test', 1443444240000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'end', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b3', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443957420000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'end', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b4', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443959220000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'end', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b5', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443959220000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'end', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b6', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443961020000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'end', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b7', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443961020000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'end', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b8', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443962820000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'end', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b9', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443962820000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'end', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b10', 30);
                    INSERT INTO metrics.rawnodemetrics (timebucket, environment, executiondate, clientid, dagid, dagversionid, nodeid, requestid, executiontimemillis)
                    VALUES(1443916800000, 'test', 1443962820000, ${dag.clientId}, ${dag.dagId}, ${dag.versionId}, 'end', 'b0a383f1-8408-7c3f-33cb-71a8d0ace0b11', 30);
                    APPLY BATCH;"""
    session.execute(query)
    session.execute(query2)
  }

  def insertEnrichedProfile(clientID: UUID, profileType: String)(implicit session:Session): Unit = {

    def queryZ(email: String, version: Int): String =
      s"""INSERT INTO profilingservice.erisprofile (clientid, userid, version, profiledata)
      values ($clientID, '$email', $version, '{"cli_id":"3307","cli_gender":"female","cli_firstname":"Ang","cli_middlename":null,"cli_lastname":"Hendriks","cli_company":null,"cli_street":null,"cli_street_number":null,"cli_zipcode":null,"cli_city":null,"cli_country":null,"cli_email":"ANG.HENDRIKS@VERSATEL.NL","cli_phone":null,"cli_shipping_company":null,"cli_shipping_gender":"none","cli_shipping_firstname":null,"cli_shipping_lastname":null,"cli_shipping_street":null,"cli_shipping_street_number":null,"cli_shipping_zipcode":null,"cli_shipping_city":null,"cli_shipping_country":null,"cli_password":"8a52bb8b0c66d52b855335dff03af94b","cli_activation_code":null,"cli_status":"ENABLE","cli_waardekaart":"Y","cli_waardekaart_id":"0","cli_addedtime":"1436691560","cli_newsletter":"Y","cli_asn":"N","cli_asn_id":"0","cli_frompartner":null,"goodcoin_id":"3111","goodcoin_account":"3279048394565910","gc_id":"3110","gc_account_code":"3279048394565910","gc_account_pincode":"311334","gc_goodcoins":"20","gc_addedtime":"1436691560"}')""";
    def queryP(email: String, version: Int): String =
      s"""INSERT INTO profilingservice.erisprofile (clientid, userid, version, profiledata)
      values ($clientID, '$email', $version, '{"cli_id":"3307","cli_gender":"female","cli_firstname":"Ang","cli_middlename":null,"cli_lastname":"Hendriks","cli_company":null,"cli_street":"Eendrachtlaan","cli_street_number":120,"cli_zipcode":"3526LB","cli_city":"Utrecht","cli_country":null,"cli_email":"ANG.HENDRIKS@VERSATEL.NL","cli_phone":null,"cli_shipping_company":null,"cli_shipping_gender":"none","cli_shipping_firstname":null,"cli_shipping_lastname":null,"cli_shipping_street":null,"cli_shipping_street_number":null,"cli_shipping_zipcode":null,"cli_shipping_city":null,"cli_shipping_country":null,"cli_password":"8a52bb8b0c66d52b855335dff03af94b","cli_activation_code":null,"cli_status":"ENABLE","cli_waardekaart":"Y","cli_waardekaart_id":"0","cli_addedtime":"1436691560","cli_newsletter":"Y","cli_asn":"N","cli_asn_id":"0","cli_frompartner":null,"goodcoin_id":"3111","goodcoin_account":"3279048394565910","gc_id":"3110","gc_account_code":"3279048394565910","gc_account_pincode":"311334","gc_goodcoins":"20","gc_addedtime":"1436691560"}')""";
    def queryN(email: String): String =
      s"""INSERT INTO profilingservice.nonprofileuser (clientid, userid, data)
      values ($clientID, '$email', {'productCount' : '5'})""";
    def queryM(mailchimpId: String, email: String): String =
      s"""INSERT INTO profilingservice.nonprofileuser (clientid, userid, data)
      values ($clientID, '$mailchimpId', {'email': '$email', 'emailId': 'c493ee4ee2', 'memberId': '9107fdcf390810af28892479b3bb1c57'})""";

    profileType match {
      case "noPC" => session.execute(queryZ("test1@test.com", 1))
        session.execute(queryN("test1@test.com"))
        session.execute(queryM("79e656c77e","test1@test.com"))
      case "PC" => session.execute(queryP("daan@equeris.com", 1))
        session.execute(queryN("daan@equeris.com"))
        session.execute(queryM("79e656c77e","daan@equeris.com"))
      case "Batch" => session.execute(queryZ("test1@test.nl", 1))
        session.execute(queryZ("test2@test.nl", 1))
        session.execute(queryZ("test3@test.nl", 1))
        session.execute(queryZ("test4@test.nl", 1))
        session.execute(queryZ("test5@test.nl", 1))
        session.execute(queryP("test6@test.nl", 1))
        session.execute(queryP("test7@test.nl", 1))
        session.execute(queryP("test8@test.nl", 1))
        session.execute(queryP("test9@test.nl", 1))
        session.execute(queryP("test10@test.nl", 1))
      case _ => println("Invalid option")
    }
  }

  def insertEanBase64IntoDB(clientId: UUID, used: Boolean)(implicit session:Session): Unit = {

    val images = List("123456789012","234567890123","345678901234","456789012345")
    val gcimages = List("100111112605", "100111112606", "100111112607", "100111112608", "100111112609", "100111112610", "100111112611", "100111112612", "100111112613", "100111112614", "100111112615", "100111112616", "100111112617", "100111112618", "100111112619", "100111112620", "100111112621", "100111112622", "100111112623", "100111112624", "100111112625", "100111112626", "100111112627", "100111112628", "100111112629", "100111112630", "100111112631", "100111112632", "100111112633", "100111112634", "100111112635", "100111112636", "100111112637", "100111112638", "100111112639", "100111112640", "100111112641", "100111112642", "100111112643", "100111112644", "100111112645", "100111112646", "100111112647", "100111112648", "100111112649", "100111112650", "100111112651", "100111112652", "100111112653", "100111112654", "100111112655", "100111112656", "100111112657", "100111112658", "100111112659", "100111112660", "100111112661", "100111112662", "100111112663", "100111112664", "100111112665", "100111112666", "100111112667", "100111112668", "100111112669", "100111112670", "100111112671", "100111112672", "100111112673", "100111112674", "100111112675", "100111112676", "100111112677", "100111112678", "100111112679", "100111112680", "100111112681", "100111112682", "100111112683", "100111112684", "100111112685", "100111112686", "100111112687", "100111112688", "100111112689", "100111112690", "100111112691", "100111112692", "100111112693", "100111112694", "100111112695", "100111112696", "100111112697", "100111112698", "100111112699", "100111112700", "100111112701", "100111112702", "100111112703", "100111112704", "100111112705", "100111112706", "100111112707", "100111112708", "100111112709", "100111112710", "100111112711", "100111112712", "100111112713", "100111112714", "100111112715", "100111112716", "100111112717", "100111112718", "100111112719", "100111112720", "100111112721", "100111112722", "100111112723", "100111112724", "100111112725", "100111112726", "100111112727", "100111112728", "100111112729", "100111112730", "100111112731", "100111112732", "100111112733", "100111112734", "100111112735", "100111112736", "100111112737", "100111112738", "100111112739", "100111112740", "100111112741", "100111112742", "100111112743", "100111112744", "100111112745", "100111112746", "100111112747", "100111112748", "100111112749", "100111112750", "100111112751", "100111112752", "100111112753", "100111112754", "100111112755", "100111112756", "100111112757", "100111112758", "100111112759", "100111112760", "100111112761", "100111112762", "100111112763", "100111112764", "100111112765", "100111112766", "100111112767", "100111112768", "100111112769", "100111112770", "100111112771", "100111112772", "100111112773", "100111112774", "100111112775", "100111112776", "100111112777", "100111112778", "100111112779", "100111112780", "100111112781", "100111112782", "100111112783", "100111112784", "100111112785", "100111112786", "100111112787", "100111112788", "100111112789", "100111112790", "100111112791", "100111112792", "100111112793", "100111112794", "100111112795", "100111112796", "100111112797", "100111112798", "100111112799", "100111112800", "100111112801", "100111112802", "100111112803", "100111112804", "100111112805", "100111112806", "100111112807", "100111112808", "100111112809", "100111112810", "100111112811", "100111112812", "100111112813", "100111112814", "100111112815", "100111112816", "100111112817", "100111112818", "100111112819", "100111112820", "100111112821", "100111112822", "100111112823", "100111112824", "100111112825", "100111112826", "100111112827", "100111112828", "100111112829", "100111112830", "100111112831", "100111112832", "100111112833", "100111112834", "100111112835", "100111112836", "100111112837", "100111112838", "100111112839", "100111112840", "100111112841", "100111112842", "100111112843", "100111112844", "100111112845", "100111112846", "100111112847", "100111112848", "100111112849", "100111112850", "100111112851", "100111112852", "100111112853", "100111112854", "100111112855", "100111112856", "100111112857", "100111112858", "100111112859", "100111112860", "100111112861", "100111112862", "100111112863", "100111112864", "100111112865", "100111112866", "100111112867", "100111112868", "100111112869", "100111112870", "100111112871", "100111112872", "100111112873", "100111112874", "100111112875", "100111112876", "100111112877", "100111112878", "100111112879", "100111112880", "100111112881", "100111112882", "100111112883", "100111112884", "100111112885", "100111112886", "100111112887", "100111112888", "100111112889", "100111112890", "100111112891", "100111112892", "100111112893", "100111112894", "100111112895", "100111112896", "100111112897", "100111112898", "100111112899", "100111112900", "100111112901", "100111112902", "100111112903", "100111112904", "100111112905", "100111112906", "100111112907", "100111112908", "100111112909", "100111112910", "100111112911", "100111112912", "100111112913", "100111112914", "100111112915", "100111112916", "100111112917", "100111112918", "100111112919", "100111112920", "100111112921", "100111112922", "100111112923", "100111112924", "100111112925", "100111112926", "100111112927", "100111112928", "100111112929", "100111112930", "100111112931", "100111112932", "100111112933", "100111112934", "100111112935", "100111112936", "100111112937", "100111112938", "100111112939", "100111112940", "100111112941", "100111112942", "100111112943", "100111112944", "100111112945", "100111112946", "100111112947", "100111112948", "100111112949", "100111112950", "100111112951", "100111112952", "100111112953", "100111112954", "100111112955", "100111112956", "100111112957", "100111112958", "100111112959", "100111112960", "100111112961", "100111112962", "100111112963", "100111112964", "100111112965", "100111112966", "100111112967", "100111112968", "100111112969", "100111112970", "100111112971", "100111112972", "100111112973", "100111112974", "100111112975", "100111112976", "100111112977", "100111112978", "100111112979", "100111112980", "100111112981", "100111112982", "100111112983", "100111112984", "100111112985", "100111112986", "100111112987", "100111112988", "100111112989", "100111112990", "100111112991", "100111112992", "100111112993", "100111112994", "100111112995", "100111112996", "100111112997", "100111112998", "100111112999", "100111113000", "100111113001", "100111113002", "100111113003", "100111113004", "100111113005", "100111113006", "100111113007", "100111113008", "100111113009", "100111113010", "100111113011", "100111113012", "100111113013", "100111113014", "100111113015", "100111113016", "100111113017", "100111113018", "100111113019", "100111113020", "100111113021", "100111113022", "100111113023", "100111113024", "100111113025", "100111113026", "100111113027", "100111113028", "100111113029", "100111113030", "100111113031", "100111113032", "100111113033", "100111113034", "100111113035", "100111113036", "100111113037", "100111113038", "100111113039", "100111113040", "100111113041", "100111113042", "100111113043", "100111113044", "100111113045", "100111113046", "100111113047", "100111113048", "100111113049", "100111113050", "100111113051", "100111113052", "100111113053", "100111113054", "100111113055", "100111113056", "100111113057", "100111113058", "100111113059", "100111113060", "100111113061", "100111113062", "100111113063", "100111113064", "100111113065", "100111113066", "100111113067", "100111113068", "100111113069", "100111113070", "100111113071", "100111113072", "100111113073", "100111113074", "100111113075", "100111113076", "100111113077", "100111113078", "100111113079", "100111113080", "100111113081", "100111113082", "100111113083", "100111113084", "100111113085", "100111113086", "100111113087", "100111113088", "100111113089", "100111113090", "100111113091", "100111113092", "100111113093", "100111113094", "100111113095", "100111113096", "100111113097", "100111113098", "100111113099")

    for(image <- images) {
      println(s"$image")
      val bytes = Files.readAllBytes(Paths.get(s"src/test/resources/flows/eans/$image.png"))
      val bytes64 = Base64.encodeBase64(bytes)
      val base64String = new String(bytes64)
      val query =
        s"""INSERT INTO profilingservice.eans (clientid, used, ean, image)
        values ($clientId, $used, '$image', '$base64String')"""
      println("query =" + query)
      session.execute(query)
    }
  }

  def insertSessions(clientId:UUID)(implicit session:Session): Unit = {

    def insertSessions(email: String, timestamp: String, epoch: String) : String =
      s"""
          INSERT INTO profilingservice.usersession (clientid, userid, sessionids, timestamps)
          values ($clientId, '$email', $timestamp, $epoch)"""
    //    def insertSessions(email: String) : String =
    //        s"""
    //            INSERT INTO profilingservice.usersession (clientid, userid, sessionids, timestamps)
    //            values ($clientId, '$email', ['1234','1235'],[1234,1235])"""

    def createTimestampAndEpoch(minMonth: List[Int]): (String, String) = {

      val timestampString = new StringBuilder
      val timestampEpoch = new StringBuilder

      timestampString.append("['")
      timestampEpoch.append("[")

      for (months <- minMonth.indices) {
        val timestamp = DateTime.now.minusMonths(minMonth(months)).getMillis
        if(months == (minMonth.length - 1)) {
          timestampEpoch.append(timestamp)
          timestampString.append(timestamp.toString)
        } else {
          timestampEpoch.append(timestamp + ",")
          timestampString.append(timestamp.toString + "','")
        }
      }

      timestampString.append("']")
      timestampEpoch.append("]")


      (timestampString.toString, timestampEpoch.toString)
    }

    val test1 = createTimestampAndEpoch(List(1,2))
    session.execute(insertSessions("test1@test.com",test1._1, test1._2))
    val test2 = createTimestampAndEpoch(List(5))
    session.execute(insertSessions("test2@test.com",test2._1, test2._2))
    val test3 = createTimestampAndEpoch(List(0))
    session.execute(insertSessions("test3@test.com",test3._1, test3._2))
    val test4 = createTimestampAndEpoch(List(3,4,5))
    session.execute(insertSessions("test4@test.com",test4._1, test4._2))
    val test5 = createTimestampAndEpoch(List(10,11,12))
    session.execute(insertSessions("test5@test.com",test5._1, test5._2))

  }

  def insertGoodclubPortalData(clientId: UUID)(implicit session:Session): Unit = {

    val userId = "test1@test.com"

    val query =
      s"""INSERT INTO profilingservice.segmentpromotions (clientid, segment, userid)
        values ($clientId, 'C', '$userId')"""
    val query2 =
      s"""INSERT INTO profilingservice.segmentviewedvariants (clientid, segment, variant, userid)
        values ($clientId, 'C', 'Variant1', '$userId')"""

    session.execute(query)
    session.execute(query2)
  }

  def insertGoodclubPortalUsers(clientId: UUID)(implicit session:Session): Unit = {


    def insertUser(email: String, version: Int, mailchimpId: String, segment: String, variants: String, numberBought: String): String =
      s"""
          BEGIN BATCH
          INSERT INTO profilingservice.erisprofile (clientid, userid, version, profiledata)
          values ($clientId, '$email', $version, '{"cli_id":"3307","cli_gender":"female","cli_firstname":"Ang","cli_middlename":null,"cli_lastname":"Hendriks","cli_company":null,"cli_street":null,"cli_street_number":null,"cli_zipcode":null,"cli_city":null,"cli_country":null,"cli_email":"ANG.HENDRIKS@VERSATEL.NL","cli_phone":null,"cli_shipping_company":null,"cli_shipping_gender":"none","cli_shipping_firstname":null,"cli_shipping_lastname":null,"cli_shipping_street":null,"cli_shipping_street_number":null,"cli_shipping_zipcode":null,"cli_shipping_city":null,"cli_shipping_country":null,"cli_password":"8a52bb8b0c66d52b855335dff03af94b","cli_activation_code":null,"cli_status":"ENABLE","cli_waardekaart":"Y","cli_waardekaart_id":"0","cli_addedtime":"1436691560","cli_newsletter":"Y","cli_asn":"N","cli_asn_id":"0","cli_frompartner":null,"goodcoin_id":"3111","goodcoin_account":"3279048394565910","gc_id":"3110","gc_account_code":"3279048394565910","gc_account_pincode":"311334","gc_goodcoins":"20","gc_addedtime":"1436691560", "segment": "$segment", "variants": "$variants"}');
          INSERT INTO profilingservice.nonprofileuser (clientid, userid, data)
          values ($clientId, '$email', {'productCount' : '$numberBought'});
          INSERT INTO profilingservice.nonprofileuser (clientid, userid, data)
          values ($clientId, '$mailchimpId', {'email': '$email', 'emailId': 'something', 'memberId': 'somethingsomething'});
          APPLY BATCH;"""

    def insertActiveSessions(email: String, minMonths: List[Int]): Unit = {

      for (month <- minMonths) {
        val timestamp = DateTime.now.minusMonths(month).getMillis
        val usersession = timestamp.toString

        val query =
          s"""
           INSERT INTO profilingservice.activeuserssession(clientid, timestamp, userid, sessionid)
           values ($clientId, $timestamp, '$email', '$usersession')
          """

        session.execute(query)
      }
    }

    session.execute(insertUser("test1@test.com", 1, "1aaaaaaaaa", "C", "Variant1", "5"))
    insertActiveSessions("test1@test.com", List(0,1,2,3))
    session.execute(insertUser("test2@test.com", 1, "2aaaaaaaaa", "C", "Variant1", "1"))
    insertActiveSessions("test2@test.com", List(1,2,3))
    session.execute(insertUser("test3@test.com", 1, "3aaaaaaaaa", "D1", "Variant1", "2"))
    insertActiveSessions("test3@test.com", List(0,3,10,11,12))
    session.execute(insertUser("test4@test.com", 1, "4aaaaaaaaa", "D1", "Variant2", "1"))
    insertActiveSessions("test4@test.com", List(3,4,5,6))
    session.execute(insertUser("test5@test.com", 1, "5aaaaaaaaa", "D1", "Variant2", "10"))
    insertActiveSessions("test5@test.com", List(0,3,11,12))

  }

  def insertMailchimpPortalData(clientId: UUID)(implicit session:Session): Unit = {

    val now = DateTime.now
    val format = DateTimeFormat.forPattern("yyyy-MM-dd")
    val timestamp = format.print(now)

    val query =
      s"""
          UPDATE profilingservice.counter
          SET counter = counter + 100
          WHERE clientId=$clientId and key='eeee2528cb-31521-nrofuserswithmultipleproducts-TIMESTAMPt00:00:00.000z';
       """.replace("TIMESTAMP", timestamp)
    val query2 =
      s"""
          UPDATE profilingservice.counter
          SET counter = counter + 500
          WHERE clientId=$clientId and key='eeee2528cb-31521-nrofuserswithproduct-TIMESTAMPt00:00:00.000z';
       """.replace("TIMESTAMP", timestamp)
    val query3 =
      s"""
          UPDATE profilingservice.counter
          SET counter = counter + 10
          WHERE clientId=$clientId and key='eeee2528cb-31517-nrofuserswithmultipleproducts-TIMESTAMPt00:00:00.000z';
       """.replace("TIMESTAMP", timestamp)
    val query4 =
      s"""
          UPDATE profilingservice.counter
          SET counter = counter + 40
          WHERE clientId=$clientId and key='eeee2528cb-31517-nrofuserswithproduct-TIMESTAMPt00:00:00.000z';
       """.replace("TIMESTAMP", timestamp)
    session.execute(query)
    session.execute(query2)
    session.execute(query3)
    session.execute(query4)
  }

  def insertAuthenticationUsers(ClientA31: Boolean, Client658: Boolean, ClientGC: Boolean)(implicit session:Session): Unit = {


    val queryClientA31 =
      s"""BEGIN BATCH
          INSERT INTO clientservicesiam.clients (clientid, enabled, fullname, userroles)
          values (a31c4772-f4ba-11e4-b9b2-1697f925ec7b, True, 'MerchantW', {'jos.dirksen@equeris.com': 'ADMIN', 'leon.derks@equeris.com': 'ADMIN', 'leonderks@hotmail.com': 'ADMIN', 'elwin.bijl@equeris.com': 'ADMIN', 'jarno.van.munster@equeris.com': 'ADMIN', 'daan@equeris.com': 'ADMIN', 'bobby@BobbyTestApp.onmicrosoft.com': 'ADMIN'});
          APPLY BATCH;"""
    val queryClient658 =
      s"""BEGIN BATCH
          INSERT INTO clientservicesiam.clients (clientid, enabled, fullname, userroles)
          values (658680e5-1c06-444c-a09d-26162fd19b9f, True, 'MerchantV', {'jarnotest@BobbyTestApp.onmicrosoft.com': 'ADMIN'});
          APPLY BATCH;"""
    val queryClientGC =
      s"""BEGIN BATCH
          INSERT INTO clientservicesiam.clients (clientid, enabled, fullname, userroles)
          values (562e67d2-8ee3-4387-874f-785375b59d33, True, 'Goodclub', {'goodclub-eris@outlook.com': 'USER', 'jos.dirksen@equeris.com': 'ADMIN', 'leon.derks@equeris.com': 'ADMIN', 'leonderks@hotmail.com': 'ADMIN', 'elwin.bijl@equeris.com': 'ADMIN', 'jarno.van.munster@equeris.com': 'ADMIN', 'daan@equeris.com': 'ADMIN'});
          APPLY BATCH;"""

    val queryUsers =
      s"""BEGIN BATCH
          INSERT INTO clientservicesiam.users (userid, enabled, name)
          values ('jos.dirksen@equeris.com', True, 'Jos');
          INSERT INTO clientservicesiam.users (userid, enabled, name)
          values ('leon.derks@equeris.com', True, 'Leon');
          INSERT INTO clientservicesiam.users (userid, enabled, name)
          values ('elwin.bijl@equeris.com', True, 'Elwin');
          INSERT INTO clientservicesiam.users (userid, enabled, name)
          values ('jarno.van.munster@equeris.com', True, 'Jarno');
          INSERT INTO clientservicesiam.users (userid, enabled, name)
          values ('daan@equeris.com', True, 'Daan');
          INSERT INTO clientservicesiam.users (userid, enabled, name)
          values ('bobby@BobbyTestApp.onmicrosoft.com', True, 'MerchantV');
          INSERT INTO clientservicesiam.users (userid, enabled, name)
          values ('jarnotest@BobbyTestApp.onmicrosoft.com', True, 'MerchantW');
          INSERT INTO clientservicesiam.users (userid, enabled, name)
          values ('goodclub-eris@outlook.com', True, 'Goodclub');
          APPLY BATCH;"""

    if(ClientA31) {
      session.execute(queryClientA31)
    }
    if(Client658) {
      session.execute(queryClient658)
    }
    if(ClientGC) {
      session.execute(queryClientGC)
    }
    session.execute(queryUsers)
  }

  def insertProductCountsPortalData(clientId: UUID)(implicit session:Session): Unit = {

    def query(userId: String, numberBought: Int): Unit = {
      val query =
        s"""
         INSERT INTO profilingservice.nonprofileuser (clientid, userid, data)
         values ($clientId, '$userId', {'productCount':'$numberBought'});"""
      session.execute(query)
    }
    //Rotterdam Segment users: test1@test.nl, test2@test.nl, test4@test.nl, test1@test.com, test3@test.com
    query("test1@test.nl", 1)
    query("test2@test.nl", 1)
    query("test4@test.nl", 2)
    query("test1@test.com", 10)
    query("test3@test.com", 100)
  }

  def insertAggregatedMailchimpPortalData(clientId: UUID, userId: String)(implicit session:Session): Unit = {

    def query =
      s"""
      INSERT INTO profilingservice.nonprofileuser (clientid, userid, data)
      values ($clientId, '$userId', {'0': '0', '1': '1', '2': '6531', '3': '721', '4': '2127', '5': '1879', 'avgClickRate': '0.05381067590371988', 'avgOpenRate': '0.3646603517186332'});"""
    session.execute(query)
  }
}
