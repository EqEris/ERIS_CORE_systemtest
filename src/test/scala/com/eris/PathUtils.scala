package com.eris

import com.eris.Environments._

object PathUtils extends Config {

  def executeDefaultVersion(dag: DAG, environment: String = "test") = s"$executorUri/clients/${dag.clientId}/dags/${dag.dagId}/default?environment=$environment"

  def executeDagVersion(dag: DAG, environment: String = "test") = s"$executorUri/clients/${dag.clientId}/dags/${dag.dagId}/versions/${dag.versionId}?environment=$environment"

  def executeDagVersionMtoM(dag: DAG, environment: String = "test") = s"$executorMtoMUri/clients/${dag.clientId}/dags/${dag.dagId}/versions/${dag.versionId}?environment=$environment"

  def executeDagVersionOAuth(dag: DAG, environment: String = "test") = s"$executorUri/clients/${dag.clientId}/dags/${dag.dagId}/versions/${dag.versionId}?environment=$environment"

  def executeAnotherClientsDagVersion(client1DAG: DAG, client2DAG: DAG, environment: String = test) = s"$executorUri/clients/${client1DAG.clientId}/dags/${client2DAG.dagId}/versions/${client2DAG.versionId}?environment=$environment"

  def storageCreate(clientId: String) = s"$storageUri/clients/$clientId/dags"

  def storageCreateMtoM(clientId: String) = s"$storageMtoMUri/clients/$clientId/dags"

  def storageDagVersion(dag: DAG) = s"$storageUri/clients/${dag.clientId}/dags/${dag.dagId}/versions/${dag.versionId}"

  def storageDagVersionMtoM(dag: DAG) = s"$storageMtoMUri/clients/${dag.clientId}/dags/${dag.dagId}/versions/${dag.versionId}"

  def storageAnotherClientsDAG(client1DAG: DAG, client2DAG: DAG) = s"$storageUri/clients/${client1DAG.clientId}/dags/${client2DAG.dagId}/versions/${client2DAG.versionId}"

  def storageAllDags(clientId: String) = s"$storageUri/clients/${clientId}/dags"

  def storageAllDagsByDomain(clientId: String, domain: String) = s"$storageUri/clients/${clientId}/dags?domain=$domain"

  def storageAllLatestDags(clientId: String) = s"$storageUri/clients/${clientId}/dags/latest?availability=true"

  def storageAllDefaultDagsEnvironment(clientId: String, environment: String) = s"$storageUri/clients/${clientId}/dags/default?environment=$environment"

  def storageAllDagVersions(dag: DAG) = s"$storageUri/clients/${dag.clientId}/dags/${dag.dagId}/versions"

  def storageDagVersionsByAvailability(dag: DAG, isAvailable: Boolean) = s"${storageAllDagVersions(dag)}?availability=$isAvailable"

  def storageDagVersionsByAvailabilityAndEnvironment(dag: DAG, isAvailable: Boolean, environment: String) = s"${storageAllDagVersions(dag)}?availability=$isAvailable&environment=$environment"

  def storageAllDefaultDagVersionsByEnvironment(dag: DAG, environment: String) = s"${storageAllDagVersions(dag)}?default&environment=$environment"

  def storageNodesQuery(clientId: String) = s"$storageUri/clients/${clientId}/nodes"

  def storageFunctionsQuery(clientId: String) = s"$storageUri/clients/${clientId}/functions"

  def storageDomainsQuery(clientId: String) = s"$storageUri/clients/${clientId}/domains"

  def storageNodeGroupsQuery(clientId: String, domain: String = "Testing") = s"$storageUri/clients/${clientId}/domains/${domain}/groups"

  def metricsDAGQuery(dag: DAG, environment: String, from: String, to: String, interval: String, maxRows: Int = 100, startAt: Int = 0) = s"$metricsUri/clients/${dag.clientId}/dags/${dag.dagId}?environment=$environment&from=$from&to=$to&interval=$interval&maxRows=$maxRows&startAt=$startAt"

  def metricsDAGVersionQuery(dag: DAG, environment: String, from: String, to: String, interval: String, maxRows: Int = 100, startAt: Int = 0) = s"$metricsUri/clients/${dag.clientId}/dags/${dag.dagId}/versions/${dag.versionId}?environment=$environment&from=$from&to=$to&interval=$interval&maxRows=$maxRows&startAt=$startAt"

  def metricsNodeQuery(dag: DAG, nodeId: String, environment: String, interval: String, from: String, to: String) = s"$metricsUri/clients/${dag.clientId}/nodes/$nodeId?environment=$environment&interval=$interval&from=$from&to=$to"

  def metricsFunctionQuery(dag: DAG, functionId: String, environment: String, interval: String, from: String, to: String) = s"$metricsUri/clients/${dag.clientId}/functions/$functionId?environment=$environment&interval=$interval&from=$from&to=$to"

  def metricsDAGNodeQuery(dag: DAG, nodeId: String, environment: String, interval: String, from: String, to: String) = s"$metricsUri/clients/${dag.clientId}/dags/${dag.dagId}/nodes/$nodeId?environment=$environment&interval=$interval&from=$from&to=$to"

  def metricsDAGFunctionQuery(dag: DAG, functionId: String, environment: String, interval: String, from: String, to: String) = s"$metricsUri/clients/${dag.clientId}/dags/${dag.dagId}/functions/$functionId?environment=$environment&interval=$interval&from=$from&to=$to"

  def metricsDAGVersionNodeQuery(dag: DAG, nodeId: String, environment: String, interval: String, from: String, to: String) = s"$metricsUri/clients/${dag.clientId}/dags/${dag.dagId}/versions/${dag.versionId}/nodes/$nodeId?environment=$environment&interval=$interval&from=$from&to=$to"

  def metricsDAGVersionFunctionQuery(dag: DAG, functionId: String, environment: String, interval: String, from: String, to: String) = s"$metricsUri/clients/${dag.clientId}/dags/${dag.dagId}/versions/${dag.versionId}/functions/$functionId?environment=$environment&interval=$interval&from=$from&to=$to"

  def metricsClientQueryFromTo(dag: DAG, environment: String, interval: String, from: String, to: String) = s"$metricsUri/clients/${dag.clientId}?environment=$environment&interval=$interval&from=$from&to=$to"

  def metricsClientQueryMtoM(dag: DAG, environment: String, interval: String, from: String, to: String) = s"$metricsMtoMUri/clients/${dag.clientId}?environment=$environment&interval=$interval&from=$from&to=$to"

  def metricsInvokeQuery(dag: DAG, requestId: String) = s"$metricsUri/clients/${dag.clientId}/requests/$requestId"

  def metricsHeatmapDAG(dag: DAG, from: String, to: String, environment: String = "test") = s"$metricsUri/clients/${dag.clientId}/dags/${dag.dagId}/heatmap?from=$from&to=$to&environment=$environment"

  def metricsHeatmapDAGVersion(dag: DAG, from: String, to: String, environment: String = "test") = s"$metricsUri/clients/${dag.clientId}/dags/${dag.dagId}/versions/${dag.versionId}/heatmap?from=$from&to=$to&environment=$environment"

  def metricsHeatmapInvoke(dag: DAG, requestId: String) = s"$metricsUri/clients/${dag.clientId}/requests/$requestId/heatmap"

  def manualInsertMetrics(dag: DAG) = s"$metricsUri/clients/${dag.clientId}/dags/${dag.dagId}?aggregate=false"

  def manualInsertMetricsManual(clientID: String) = s"$metricsUri/clients/$clientID"

  def manualRollup(from: String, to: String) = s"$metricsUri/rollup?from=$from&to=$to"

  def manualRollupMinute() = s"$metricsUri/rollup?inMemory=true"

  def manualRollupStatus() = s"$metricsUri/rollup?status=true"

  def azureAuthExecutor() = s"$executorIamUri/authenticate?redirectUrl=http://cupcat1.blogspot.com"

  def azureAuthStorage() = s"$storageIamUri/authenticate?redirectUrl=http://cupcat1.blogspot.com"

  def azureExecutorCacheKey(key: String) = s"$executorIamUri/token?cachekey=$key"

  def azureStorageCacheKey(key: String) = s"$executorIamUri/token?cachekey=$key"

}

object PathUtilsEquerisAPI extends Config {

  def sendInvokeDAGRequest(dag: DAG) = s"$APIUri/v1/executor/dags/${dag.dagId}/default?clientId=${dag.clientId}"

  def requestFromBlueconic(client: String) = s"$APIUri/v1/blueconic?clientId=$client"

  def invokeBlueconicUrl(client: String) = s"$APIUri/v1/profiling/v1/blueconic?clientId=$client&apikey=$clientGCApiKey"

}
