package com.eris

import java.util.UUID

import com.datastax.driver.core.{Row, Session}
import com.eqeris.DAGFromTemplate
import org.joda.time.DateTime


trait InsertDAG {

  def insertBlueconicPluginDAGIntoDB(clientID: UUID)(implicit session:Session): Unit = {
    val timestamp = DateTime.now.getMillis
    print(timestamp)

    val queryDag =
      s"""INSERT INTO dag.workflows (clientId, workflowId, versionId, versionAlias, parentVersionId, invocationParams, updateComment, timestamp, name, description, domain, author, available, environments, graphJson)
      VALUES(CLIENTID, 0a8aa010-15e5-11e6-b2f6-37ff2910bf34, 0a8aa011-15e5-11e6-b2f6-37ff2910bf34, '1.0.0', null, '{ "mailchimpId": "someMailchimpId", "visits": 5, "profileId": "someBlueconicProfileId", "channel": "goodclub.nl", "recentCity": "Utrecht", "pluginId": "segmentTracker", "origin": "blueconic", "productClick": "someProduct", "userId": "someUserId@mail.com", "categoryClick": "someCategory", "merchant": "goodclub" }', '', $timestamp, 'GoodClub-location-favorite-engagement-DAG', 'This DAG adds the Goodclub user to a mailchimpGroup, calculates the user his favourite products and calculates the user his engagementrate', 'All', 'Eris', true, { 'test','staging','prod' }, 'GOODCLUBNODES');""".stripMargin.replace("CLIENTID", clientID.toString).replace("GOODCLUBNODES", DAGFromTemplate(goodclubNodes).string)
    val queryVersions =
      s"""INSERT INTO dag.environmentVersions (clientId, workflowId, environment, versionId)
      VALUES($clientID, 0a8aa010-15e5-11e6-b2f6-37ff2910bf34, 'test', 0a8aa011-15e5-11e6-b2f6-37ff2910bf34);"""
    val queryVersionId =
      s"""UPDATE dag.latest SET latestVersionId = 0a8aa011-15e5-11e6-b2f6-37ff2910bf34 WHERE clientId = $clientID AND workflowId = 0a8aa010-15e5-11e6-b2f6-37ff2910bf34;"""
    val queryDefault =
      s"""UPDATE dag.default SET defaultVersionId = 0a8aa011-15e5-11e6-b2f6-37ff2910bf34 WHERE clientId = $clientID AND environment = 'test' AND workflowId = 0a8aa010-15e5-11e6-b2f6-37ff2910bf34;"""
    session.execute(queryDag)
    session.execute(queryVersions)
    session.execute(queryVersionId)
    session.execute(queryDefault)
  }

  def insertBlueconicEANDAGIntoDB(clientID: UUID)(implicit session:Session): Unit = {
    val timestamp = DateTime.now.getMillis
    print(timestamp)

    val queryDag =
      s"""INSERT INTO dag.workflows (clientId, workflowId, versionId, versionAlias, parentVersionId, invocationParams, updateComment, timestamp, name, description, domain, author, available, environments, graphJson)
      VALUES(CLIENTID, 89130d00-15e5-11e6-b2f6-37ff2910bf34, 89130d01-15e5-11e6-b2f6-37ff2910bf34, '1.0.0', null, '{ "eanEmail": "someEmailEAN", "mailchimpId": "someMailchimpId", "userId": "SomeEmail" }', '', $timestamp, 'GoodClub-EAN-DAG', 'Sends an email with a Goodclub EAN code to the customer if conditions are met','All', 'Eris', true, { 'test','staging','prod' }, 'EANNODES');""".stripMargin.replace("CLIENTID", clientID.toString).replace("EANNODES", DAGFromTemplate(goodclubEANNodes).string)
    val queryVersions =
      s"""INSERT INTO dag.environmentVersions (clientId, workflowId, environment, versionId)
      VALUES($clientID, 89130d00-15e5-11e6-b2f6-37ff2910bf34, 'test', 89130d01-15e5-11e6-b2f6-37ff2910bf34);"""
    val queryVersionId =
      s"""UPDATE dag.latest SET latestVersionId = 89130d01-15e5-11e6-b2f6-37ff2910bf34 WHERE clientId = $clientID AND workflowId = 89130d00-15e5-11e6-b2f6-37ff2910bf34;"""
    val queryDefault =
      s"""UPDATE dag.default SET defaultVersionId = 89130d01-15e5-11e6-b2f6-37ff2910bf34 WHERE clientId = $clientID AND environment = 'test' AND workflowId = 89130d00-15e5-11e6-b2f6-37ff2910bf34;"""
    session.execute(queryDag)
    session.execute(queryVersions)
    session.execute(queryVersionId)
    session.execute(queryDefault)
  }

  def insertBlueconicMainDAGIntoDB(clientID: UUID)(implicit session:Session): Unit = {
    val timestamp = DateTime.now.getMillis
    print(timestamp)

    val queryDag =
      s"""INSERT INTO dag.workflows (clientId, workflowId, versionId, versionAlias, parentVersionId, invocationParams, updateComment, timestamp, name, description, domain, author, available, environments, graphJson)
      VALUES(CLIENTID, e8f1b180-1da2-11e6-a071-1b563aec070e, e8f1b181-1da2-11e6-a071-1b563aec070e, '1.0.0', null, '{ "visits": 50, "profileId": "someBlueconicProfileId", "channel": "goodclub.nl", "recentCity": "Utrecht", "pluginId": "segmentTracker", "eanEmail": "someEAN@email", "origin": "blueconic", "sessionId": "someSession", "productClick": "someProduct", "userId": "test@test.com", "categoryClick": "someCategory", "merchant": "goodclub", "Segment": "C", "variantsViewed": "someVariant"}', '', $timestamp, 'GoodClub-main-DAG', 'DAG that handles incoming Goodclub requests and which invokes the location-favorite-engagement-DAG and the EAN-DAG when an EAN-email is provided','All', 'Eris', true, { 'test','staging','prod' }, 'MAINNODES');""".stripMargin.replace("CLIENTID", clientID.toString).replace("MAINNODES", DAGFromTemplate(goodclubMainNodes).string)
    val queryVersions =
      s"""INSERT INTO dag.environmentVersions (clientId, workflowId, environment, versionId)
      VALUES($clientID, e8f1b180-1da2-11e6-a071-1b563aec070e, 'test', e8f1b181-1da2-11e6-a071-1b563aec070e);"""
    val queryVersionId =
      s"""UPDATE dag.latest SET latestVersionId = e8f1b181-1da2-11e6-a071-1b563aec070e WHERE clientId = $clientID AND workflowId = e8f1b180-1da2-11e6-a071-1b563aec070e;"""
    val queryDefault =
      s"""UPDATE dag.default SET defaultVersionId = e8f1b181-1da2-11e6-a071-1b563aec070e WHERE clientId = $clientID AND environment = 'test' AND workflowId = e8f1b180-1da2-11e6-a071-1b563aec070e;"""
    session.execute(queryDag)
    session.execute(queryVersions)
    session.execute(queryVersionId)
    session.execute(queryDefault)
  }
}
