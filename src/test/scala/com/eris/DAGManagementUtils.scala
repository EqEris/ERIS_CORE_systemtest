package com.eris

import java.nio.charset.StandardCharsets

import argonaut._
import argonaut.CodecJson._
import Argonaut._
import com.eris.Environments._
import com.eris.PathUtils.{UATDag1 => _, UATDag2 => _, UATDag2B => _, UATDag2Delay => _, UATDag3 => _, UATDag6 => _, calculationNodeDag => _, completeCalculationDAG => _, completeCalculationDAGV1 => _, decisionNodeDag => _, fourDecisionsDag => _, fourDecisionsDag2 => _, googleMapsSimple => _, googleMapsSimpleTemplate => _, googleMapsSimpleVersionTemplate => _, multiBranchDag => _, parallelDag => _, pythagorasDag => _, pythagorasDagNoRoot => _, startStop => _, startStopV1Template => _, startStopV2Template => _, timeoutService => _, _}
import com.eris.Templates.DAGFromTemplate
import com.jayway.restassured.RestAssured
import com.jayway.restassured.http.ContentType
import com.jayway.restassured.response.Response
import org.scalatest._

object DAGStorageUtils extends Matchers {

  def callStorage(storageURI: String, postData: String, token: String): Response = {
    RestAssured
      .given
      .header("Authorization", token)
      .body(postData)
      .contentType(ContentType.JSON.withCharset(StandardCharsets.UTF_8))
      .when
      .request
      .post(s"$storageURI")
  }

  def callStorageDelete(storageURI: String, token: String): Response = {
    RestAssured
      .given
      .header("Authorization", token)
      .when
      .request
      .delete(s"$storageURI")
  }

  val unique = java.util.UUID.randomUUID()

  def configNodeMappings(names: List[String], values: List[String], templateName: String): Map[String, String] = {
    val test = new DAGFromTemplate("", Map.empty)

    var toMap: Map[String, String] = Map("<NAME>" -> (templateName + java.util.UUID.randomUUID().toString))
    for (i <- names.indices) {
      val name = names(i)
      val value = values(i)
      toMap += (s"$name" -> value)
    }
    toMap
  }

  def storeDag(template: String): DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(template, Map("<NAME>" -> (template + java.util.UUID.randomUUID().toString))).string)

  def storeConfigDAG(template: String, configNames: List[String], configValues: List[String], nameOfDAG: String): DAG = {
    storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(template, configNodeMappings(configNames, configValues, nameOfDAG)).string)
  }

  def storeFourDecisionsDag: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(fourDecisionsDag, Map("<NAME>" -> ("4-decisions-dag" + java.util.UUID.randomUUID().toString))).string)

  def storeFourDecisionsDag2Versions: (DAG, DAG) = store2FourDecisionsDAG2Versions(client1ClientId, validAuthenticationTokenClient1)

  def storeMaps(clientID: String, clientAuth: String): DAG = storeDAG(clientID, clientAuth, DAGFromTemplate(googleMapsSimple, Map("<NAME>" -> ("multiBranchDag" + java.util.UUID.randomUUID().toString))).string)

  def storeMapsForClient2: DAG = storeDAG(client2ClientId, validAuthenticationTokenClient2, DAGFromTemplate(googleMapsSimple).string)

  def storeStartStop: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(startStop).string)

  def storeTimeout: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(timeoutService).string)

  def storeComplexMultiBranchDag: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(multiBranchDag, Map("<NAME>" -> ("multiBranchDag" + java.util.UUID.randomUUID().toString))).string)

  def storePythagorasDag: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(pythagorasDag, Map("<NAME>" -> java.util.UUID.randomUUID().toString)).string)

  def storePythagorasNoRootDAG: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(pythagorasDagNoRoot).string)

  def storeParallelDag: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(parallelDag).string)

  def storeUATDag1: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(UATDag1).string)

  def storeUATDag1MtoM: DAG = storeDAGMtoM(client1ClientId, DAGFromTemplate(UATDag1).string)

  def storeUATDag1Oauth(authToken: String): DAG = storeDAG(client1ClientId, authToken, DAGFromTemplate(UATDag1).string)

  def storeUATDag2: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(UATDag2).string)

  def store2UATDag2Versions: (DAG, DAG) = store2UATDag2Versions(client1ClientId, validAuthenticationTokenClient1)

  def storeUATDag2Delay: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(UATDag2Delay).string)

  def storeUATDag3: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(UATDag3).string)

  def storeUATDag6: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(UATDag6).string)

  def storeDagWithDecisionNode: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(decisionNodeDag).string)

  def storeDagWithCalculationNode: DAG = storeDAG(client1ClientId, validAuthenticationTokenClient1, DAGFromTemplate(calculationNodeDag).string)

  def storeCompleteDagWithCalculationNodeGetResponse: Response = storeDAGGetRestResponse(validAuthenticationTokenClient1, DAGFromTemplate(completeCalculationDAG).string, createDAGUriForClient(client1ClientId))

  def storeCompleteDagWithCalculationNodeGetResponseVersion(parentWorkflowId: String, parentVersionId: String): Response = {
    val params = Map[String, String]("<WORKFLOW_ID>" -> parentWorkflowId, "<PARENT_VERSION_ID>" -> parentVersionId)
    storeDAGGetRestResponse(validAuthenticationTokenClient1, DAGFromTemplate(completeCalculationDAGV1, params).string, createDAGUriForClient(client1ClientId))
  }

  def store3StartStopVersions: (DAG, DAG, DAG) = store3StartStopVersions(client1ClientId, validAuthenticationTokenClient1)

  def store3StartStopVersionsForClient2: (DAG, DAG, DAG) = store3StartStopVersions(client2ClientId, validAuthenticationTokenClient1)

  def store4GoogleMapsVersions: (DAG, DAG, DAG, DAG) = store4GoogleMapsVersions(client1ClientId, validAuthenticationTokenClient1)

  def store2UnavailableGoogleMapsVersions: (DAG, DAG) = store2UnavailableGoogleMapsVersions(client1ClientId, validAuthenticationTokenClient1)

  private def store3StartStopVersions(clientId: String, authToken: String): (DAG, DAG, DAG) = {

    val dag1 = storeDAG(clientId, authToken, DAGFromTemplate(startStop).string)
    val params = Map[String, String]("<CLIENT_ID>" -> dag1.clientId, "<WORKFLOW_ID>" -> dag1.dagId, "<PARENT_VERSION_ID>" -> dag1.versionId)

    val dag2 = storeDAGVersionGetDAGResponse(dag1, authToken, DAGFromTemplate(startStopV1Template, params).string)
    val dag3 = storeDAGVersionGetDAGResponse(dag1, authToken, DAGFromTemplate(startStopV2Template, params).string)

    (dag1, dag2, dag3)
  }

  private def store4GoogleMapsVersions(clientId: String, authToken: String): (DAG, DAG, DAG, DAG) = {

    val params = Map[String, String]("<DOMAIN>" -> "Testing", """"<AVAILABLE>"""" -> "true", "<ENVIRONMENTS>" -> s"""$test", "$staging", "$prod""")
    val template = DAGFromTemplate(googleMapsSimpleTemplate, params).string
    val dag1 = storeDAG(client1ClientId, validAuthenticationTokenClient1, template)

    val version2Params = params + ("<PARENT_VERSION_ID>" -> dag1.dagId)
    val version2Template = DAGFromTemplate(googleMapsSimpleVersionTemplate, version2Params).string
    val dag2 = storeDAGVersionGetDAGResponse(dag1, authToken, version2Template)

    val version3Params = version2Params + (""""<AVAILABLE>"""" -> "false")
    val version3Template = DAGFromTemplate(googleMapsSimpleVersionTemplate, version3Params).string
    val dag3 = storeDAGVersionGetDAGResponse(dag1, authToken, version3Template)

    val version4Params = version3Params +("<DOMAIN>" -> "Loyalty", """"<AVAILABLE>"""" -> "true", "<ENVIRONMENTS>" -> s"""$test""")
    val version4Template = DAGFromTemplate(googleMapsSimpleVersionTemplate, version4Params).string
    val dag4 = storeDAGVersionGetDAGResponse(dag1, authToken, version4Template)

    (dag1, dag2, dag3, dag4)
  }

  private def store2UnavailableGoogleMapsVersions(clientId: String, authToken: String): (DAG, DAG) = {

    val params = Map[String, String]("<DOMAIN>" -> "Testing", """"<AVAILABLE>"""" -> "false", "<ENVIRONMENTS>" -> s"""$test", "$staging", "$prod""")
    val template = DAGFromTemplate(googleMapsSimpleTemplate, params).string
    val dag1 = storeDAG(client1ClientId, validAuthenticationTokenClient1, template)

    val version2Params = params + ("<PARENT_VERSION_ID>" -> dag1.dagId)
    val version2Template = DAGFromTemplate(googleMapsSimpleVersionTemplate, version2Params).string
    val dag2 = storeDAGVersionGetDAGResponse(dag1, authToken, version2Template)

    (dag1, dag2)
  }

  private def store2UATDag2Versions(clientId: String, authToken: String): (DAG, DAG) = {

    val template = DAGFromTemplate(UATDag2).string
    val dag1 = storeDAG(client1ClientId, validAuthenticationTokenClient1, template)

    val version2Params = Map[String, String]("<PARENT_VERSION_ID>" -> dag1.dagId)
    val version2Template = DAGFromTemplate(UATDag2B, version2Params).string
    val dag2 = storeDAGVersionGetDAGResponse(dag1, authToken, version2Template)

    (dag1, dag2)
  }

  private def store2FourDecisionsDAG2Versions(clientId: String, authToken: String): (DAG, DAG) = {

    val template = DAGFromTemplate(fourDecisionsDag, Map("<NAME>" -> ("4-decisions-dag " + java.util.UUID.randomUUID().toString))).string
    val dag1 = storeDAG(client1ClientId, validAuthenticationTokenClient1, template)

    val version2Params = Map[String, String]("<PARENT_VERSION_ID>" -> dag1.dagId, "<NAME>" -> ("4-decisions-dag-v2 " + java.util.UUID.randomUUID().toString))
    val version2Template = DAGFromTemplate(fourDecisionsDag2, version2Params).string
    print(DAGFromTemplate(fourDecisionsDag2, version2Params).string)
    val dag2 = storeDAGVersionGetDAGResponse(dag1, authToken, version2Template)

    (dag1, dag2)
  }

  def createDAGUriForClient(clientId: String): String = storageCreate(clientId)

  def createDAGUriForClientMtoM(clientId: String): String = storageCreateMtoM(clientId)

  def client1CreateClient2DAG(clientId: String): String = storageCreate(clientId)

  def updateDAGUriForClient(dag: DAG): String = storageAllDagVersions(dag)

  private def storeDAGGetCreateResponse(clientId: String, authToken: String, dagDefinition: String) = {
    val response = storeDAGGetRestResponse(authToken, dagDefinition, createDAGUriForClient(clientId)).jsonPath.prettify
    response.decodeEither[CreateResponse].leftMap { _ => response }
  }

  private def storeDAGGetCreateResponseMtoM(clientId: String, dagDefinition: String) = {
    val response = storeDAGGetRestResponseMtoM(dagDefinition, createDAGUriForClientMtoM(clientId)).jsonPath.prettify
    response.decodeEither[CreateResponse].leftMap { _ => response }
  }

  private def storeDAG(clientId: String, authToken: String, dagDefinition: String): DAG =
    storeDAGGetCreateResponse(clientId, authToken, dagDefinition)
      .fold(errorResponse => fail(s"Invalid response from storage $errorResponse"), createResponse => DAG(clientId, createResponse.dagId, createResponse.versionId))

  private def storeDAGMtoM(clientId: String, dagDefinition: String): DAG =
    storeDAGGetCreateResponseMtoM(clientId, dagDefinition)
      .fold(errorResponse => fail(s"Invalid response from storage $errorResponse"), createResponse => DAG(clientId, createResponse.dagId, createResponse.versionId))

  private def storeDAGVersionGetCreateResponse(dag: DAG, authToken: String, dagDefinition: String) = {
    val response = storeDAGGetRestResponse(authToken, dagDefinition, updateDAGUriForClient(dag)).jsonPath.prettify
    response.decodeEither[CreateResponse].leftMap(_ => response)
  }

  def storeDAGVersionGetDAGResponse(dag: DAG, authToken: String, dagDefinition: String): DAG =
    storeDAGVersionGetCreateResponse(dag, authToken, dagDefinition)
      .fold(errorResponse => fail(s"Invalid response from storage $errorResponse"), createResponse => DAG(dag.clientId, createResponse.dagId, createResponse.versionId))

  def storeDAGGetRestResponse(authToken: String, dagDefinition: String, createUri: String): Response = {
    RestAssured
      .given
      .header("Authorization", authToken)
      .contentType(ContentType.JSON)
      .body(dagDefinition)
      .when
      .request
      .post(createUri)
  }

  def storeDAGGetRestResponseMtoM(dagDefinition: String, createUri: String): Response = {

    RestAssured
      .given.contentType(ContentType.JSON)
      .body(dagDefinition)
      .when
      .request
      .post(createUri)
  }

}

object DAGRetrievalUtils {

  def getAllDAG: Response = getWithAuth(storageAllDags(client1ClientId), validAuthenticationTokenClient1)

  def getAllDAGVersions(dag: DAG): Response = getWithAuth(storageAllDagVersions(dag), validAuthenticationTokenClient1)

  def getDAG(dag: DAG): Response = getWithAuth(storageDagVersion(dag), validAuthenticationTokenClient1)

  def getDAGWithOauth(dag: DAG, authToken: String): Response = getWithAuth(storageDagVersion(dag), authToken)

  def getDAGMtoM(dag: DAG): Response = getWithAuthMtoM(storageDagVersionMtoM(dag))

  def client1GetClient2DAG(client1DAG: DAG, client2DAG: DAG) = getWithAuth(storageAnotherClientsDAG(client1DAG, client2DAG), validAuthenticationTokenClient1)

  def getAllUnavailableVersionsOfDag(dag: DAG): Response = getWithAuth(storageDagVersionsByAvailability(dag, false), validAuthenticationTokenClient1)

  def getLatestVersionsOfAllDags: Response = getWithAuth(storageAllLatestDags(client1ClientId), validAuthenticationTokenClient1)

  def getDefaultsByEnvironment(clientId: String, environment: String) = getWithAuth(storageAllDefaultDagsEnvironment(clientId, environment), validAuthenticationTokenClient1)

  def getAllAvailableByEnvironment(dag: DAG, available: Boolean, environment: String) = getWithAuth(storageDagVersionsByAvailabilityAndEnvironment(dag, available, environment), validAuthenticationTokenClient1)

  def getAllByDomain(clientId: String, domain: String) = getWithAuth(storageAllDagsByDomain(clientId, domain), validAuthenticationTokenClient1)

  private def getDAGs(dagUri: String, validAuthenticationToken: String): Response =
    RestAssured
      .given
      .header("Authorization", validAuthenticationToken)
      .contentType(ContentType.JSON)
      .expect
      .response
      .when
      .request
      .get(dagUri)

  private def getDAG(dagUri: String, validAuthenticationToken: String): Response =
    RestAssured
      .given
      .header("Authorization", validAuthenticationToken)
      .contentType(ContentType.JSON)
      .expect
      .response
      .when
      .request
      .get(dagUri)

}

object DAGUpdateUtils {

  case class UpdateBody(available: Boolean, environments: List[String])

  object UpdateBody {
    implicit val codec = derive[UpdateBody]
  }

  def updateDAG(dag: DAG, isAvailable: Boolean, environments: List[String], AuthClientToken: String = validAuthenticationTokenClient1): Response = updateDAGWithBody(dag, UpdateBody(isAvailable, environments).asJson.toString(), AuthClientToken)

  def updateDAGWithBody(dag: DAG, putBody: String, AuthClient: String = validAuthenticationTokenClient1): Response = updateDAG(storageDagVersion(dag), putBody, AuthClient)


  private def updateDAG(dagUri: String, putBody: String, validAuthenticationToken: String): Response =

    RestAssured
      .given
      .header("Authorization", validAuthenticationToken)
      .contentType(ContentType.JSON)
      .body(putBody)
      .expect
      .response
      .when
      .request
      .put(dagUri)

  def markDAGAsUnavailable(dag: DAG) = {
    val response = updateDAG(dag, false, Nil)
    response.statusCode shouldBe 200
  }
}

