package com.eris

import java.util.UUID

import com.typesafe.config.{ConfigException, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

trait Config extends LazyLogging {

  /**
    * Application config object.
    */
  private[this] val config = ConfigFactory.load()

  /**
    * Gets the required string from the config file or throws
    * an exception if the string is not found.
    *
    * @param path path to string
    * @return string fetched by path
    */
  def getRequiredString(path: String) = {
    Try(config.getString(path)).getOrElse {
      handleError(path)
    }
  }

  /**
    * Gets the required int from the config file or throws
    * an exception if the int is not found.
    *
    * @param path path to int
    * @return int fetched by path
    */
  def getRequiredInt(path: String) = {
    Try(config.getInt(path)).getOrElse {
      handleError(path)
    }
  }

  /**
    * Gets the required string list from the config file or throws
    * an exception if the string list is not found.
    *
    * @param path path to string list
    * @return string list fetched by path
    */
  def getRequiredStringList(path: String) = {
    Try(config.getStringList(path)).getOrElse {
      handleError(path)
    }
  }

  private[this] def handleError(path: String) = {
    val errMsg = s"Missing required configuration entry: $path"
    logger.error(errMsg)
    throw new ConfigException.Missing(errMsg)
  }
  //Config
  val cassandraHost = getRequiredString("cassandra.host")
  val cassandraPort = getRequiredInt("cassandra.port")
  val contentTypeJson = Map("Content-Type" -> "application/json")

  //Services
  val storageUri = getRequiredString("service.storage.uri")
  val storageIamUri = getRequiredString("service.storage_iam.uri")
  val storageMtoMUri = getRequiredString("service.storage_mtom.uri")
  val executorUri = getRequiredString("service.executor.uri")
  val executorIamUri = getRequiredString("service.executor_iam.uri")
  val executorMtoMUri = getRequiredString("service.executor_mtom.uri")
  val metricsUri = getRequiredString("service.metrics.uri")
  val metricsMtoMUri = getRequiredString("service.metrics_mtom.uri")
  val testServicesUri = getRequiredString("service.test_services.uri")
  val adminUri = getRequiredString("service.admin.uri")
  val APIUri = getRequiredString("service.api.uri")
  val mockUri = getRequiredString("service.mock.uri")

  //Clients
  val client1ClientId = getRequiredString("authentication.basicAuth.client1.clientId")
  val basicAuthClient1Username = getRequiredString("authentication.basicAuth.client1.user")
  val basicAuthClient1Password = getRequiredString("authentication.basicAuth.client1.password")
  val client1ApiKey = getRequiredString("authentication.basicAuth.client1.apikey")

  val client2ClientId = getRequiredString("authentication.basicAuth.client2.clientId")
  val basicAuthClient2Username = getRequiredString("authentication.basicAuth.client2.user")
  val basicAuthClient2Password = getRequiredString("authentication.basicAuth.client2.password")

  val clientGCClientId = getRequiredString("authentication.basicAuth.clientGC.clientId")
  val basicAuthClientGoodclubUsername = getRequiredString("authentication.basicAuth.clientGC.user")
  val basicAuthClientGoodclubPassword = getRequiredString("authentication.basicAuth.clientGC.password")
  val clientGCApiKey = getRequiredString("authentication.basicAuth.clientGC.apikey")

  def randomClientId: String = UUID.randomUUID.toString

  //DAGs
  val randomLatencyTemplate = "random-latency-dag.json"
  val invalidStartStopTemplate = "invalid-start-stop-dag.json"
  val googleMapsSimple = "google-maps-simple-dag.json"
  val moreThanOneStartNodeTemplate = "more-than-one-start-node-dag.json"
  val moreThanOneEndNodeTemplate = "more-than-one-end-node-dag.json"
  val noStartNodeTemplate = "no-start-node-dag.json"
  val noStopNodeTemplate = "no-stop-node-dag.json"

  val pythagorasTemplate = "metadata/pythagoras-dag.json"
  val pythagorasNoNameTemplate ="metadata/pythagoras-dag-no-name.json"
  val pythagorasNoAuthorTemplate = "metadata/pythagoras-dag-no-author.json"
  val pythagorasNoDescriptionTemplate = "metadata/pythagoras-dag-no-description.json"
  val pythagorasNoDomainTemplate = "metadata/pythagoras-dag-no-domain.json"
  val pythagorasNoEnvironmentTemplate = "metadata/pythagoras-dag-no-environment.json"
  val pythagorasNoVersionAliasTemplate = "metadata/pythagoras-dag-no-versionAlias.json"
  val pythagorasWithInvocationParams = "metadata/pythagoras-dag-with-invocationParams.json"

  val startStop = "start-stop/start-stop-dag.json"
  val startStopDuplicateNameTemplate = "start-stop/start-stop-dag-duplicate-name.json"
  val startStopV1Template = "start-stop/start-stop-dag-v1.json"
  val startStopV2Template = "start-stop/start-stop-dag-v2.json"

  val updateStartStopNoStartNodeTemplate = "start-stop/update-no-start-node.json"
  val updateStartStopMultipleStartNodeTemplate = "start-stop/update-multiple-start-node.json"
  val updateStartStopNoExitNodeTemplate = "start-stop/update-no-exit-node.json"

  val googleMapsSimpleTemplate = "google-maps/google-maps-simple-dag-template.json"
  val googleMapsSimpleVersionTemplate = "google-maps/google-maps-simple-version-template.json"

  val timeoutService = "timeout/timeout-service-dag.json"
  val decisionNodeDag = "nodetests/decision-node-dag.json"
  val calculationNodeDag = "nodetests/calculation-node-dag.json"
  val pythagorasDag = "nodetests/pythagoras-dag.json"
  val pythagorasDagNoRoot = "nodetests/pythagoras-dag-no-root.json"
  val parallelDag = "nodetests/parallel-dag.json"
  val multiBranchDag = "nodetests/complex-multi-branch-dag.json"

  val UATDag1 = "UAT/DAG1-simple-calculation.json"
  val UATDag2 = "UAT/DAG2-simple-split.json"
  val UATDag2Delay = "UAT/DAG2-plusDelay.json"
  val UATDag3 = "UAT/DAG3-single-decision.json"
  val UATDag6 = "UAT/DAG6-multiple-vendors.json"

  val UATDag2B = "update/DAG2-B-simple-split.json"
  val UATDag2NoParentVersionId = "update/DAG2-noParentVersionId.json"
  val UATDag2NoAuthor = "update/DAG2-noAuthor.json"
  val UATDag2NoAvailable = "update/DAG2-noAvailable.json"
  val UATDag2NoDescription = "update/DAG2-noDescription.json"
  val UATDag2NoDomain = "update/DAG2-noDomain.json"
  val UATDag2NoEnvironments = "update/DAG2-noEnvironments.json"
  val UATDag2NoName = "update/DAG2-noName.json"
  val UATDag2NoUpdateComment = "update/DAG2-noUpdateComment.json"

  val correctConfig = "config-nodes/correct-config.json"
  val configWithWrongType = "config-nodes/config-with-wrong-type.json"
  val configWithoutParams = "config-nodes/config-without-params.json"
  val configWithExtraParams = "config-nodes/config-with-extra-params.json"

  val completeCalculationDAG = "complete-calculation-node-dag.json"
  val completeCalculationDAGV1 = "complete-calculation-dag-v1.json"

  val loopingDAGTemplate = "errors/looping-dag.json"
  val updateToLoopingDagTemplate = "errors/looping-dag-update.json"
  val openEdgeDAGTemplate = "errors/open-edge-dag.json"
  val updateToOpenEdgeDAGTemplate = "errors/open-edge-dag-update.json"
  val splitToEndDAGTemplate = "errors/split-to-end-dag.json"
  val splitToOneEdgeDAGTemplate = "errors/split-to-one-edge-dag.json"
  val splitNoIncomingEdgeDAGTemplate = "errors/split-no-incoming-edge-dag.json"
  val synchronisationFromOneEdgeDAGTemplate = "errors/synchronisation-from-one-edge-dag.json"
  val synchronisationNoOutgoingEdgeDAGTemplate = "errors/synchronisation-no-outgoing-edge-dag.json"
  val decisionNoIncomingEdgeDAGTemplate = "errors/decision-no-incoming-edge-dag.json"
  val decisionNoOutgoingEdgeDAGTemplate = "errors/decision-no-outgoing-edge-dag.json"
  val startNoOutgoingEdgeDAGTemplate = "errors/start-no-outgoing-edge-dag.json"
  val endNoIncomingEdgeDAGTemplate = "errors/end-no-incoming-edge-dag.json"
  val calculationNoIncomingEdgeDAGTemplate = "errors/calculation-no-incoming-edge-dag.json"
  val calculationNoOutgoingEdgeDAGTemplate = "errors/calculation-no-outgoing-edge-dag.json"

  val collectingUrls = "adwords/collectingcleaningstoringURL.json"
  val keywordUrls = "adwords/keywordscleaningstoringURL.json"
  val similarUrls = "adwords/similarwebcleaningstoringURL.json"
  val analyticsUrls = "adwords/googleanalyticscleaningstoring.json"

  val goodclubDag = "goodclub/blueconic-goodclub-dag.json"
  val goodclubNodes = "goodclub/blueconic-dag-nodes.json"
  val goodclubEANDag = "goodclub/blueconic-EAN-dag.json"
  val goodclubEANNodes = "goodclub/blueconic-EAN-dag-nodes.json"
  val goodclubMainDag = "goodclub/blueconic-main-dag.json"
  val goodclubMainNodes = "goodclub/blueconic-main-dag-nodes.json"

  //DAGs including Metrics
  val fourDecisionsDag = "metrics-inserts/4-decisions-dag/4-decisions-dag.json"
  val fourDecisionsDag2 = "metrics-inserts/4-decisions-dag/4-decisions-dag-v2.json"
  val path1 = "metrics-inserts/4-decisions-dag/path1.json"
  val path2 = "metrics-inserts/4-decisions-dag/path2.json"
  val path3 = "metrics-inserts/4-decisions-dag/path3.json"
  val path4 = "metrics-inserts/4-decisions-dag/path4.json"
  val path5 = "metrics-inserts/4-decisions-dag/path5.json"
  val path6 = "metrics-inserts/4-decisions-dag/path6.json"
  val metricsInsert = "metrics-inserts/insertMetrics.json"
  val metricsBulkInsert = "allnodes.json"
}
