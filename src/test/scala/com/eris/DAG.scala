package com.eris

import argonaut.Argonaut._
import argonaut.DecodeJson._
import argonaut._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

case class DAG(clientId: String, dagId: String, versionId: String)

object DAG {
  implicit def codec = derive[DAG]
}

case class CreateResponse(dagId:String, versionId:String, timestamp:String)

object CreateResponse {
  implicit def codec = derive[CreateResponse]
}

case class UpdateResponse(dagId:String, versionId:String, timestamp: String)

object UpdateResponse {
  implicit def codec = derive[UpdateResponse]
}


case class CreateNewVersionResponse(dagId: String, versionId:String, timestamp:String)


object CreateNewVersionResponse {
  implicit def codec = derive[CreateNewVersionResponse]
}

case class DAGResponse(dagId: String,
                            versionId: String,
                            parentVersionId: Option[String],
                            clientId: String,
                            name: String,
                            description: String,
                            author: String,
                            available: Boolean,
                            domain: String,
                            invocationParams: Json,
                            environments: List[String],
                            timestamp: DateTime,
                            graph: Json)


object DAGResponse {
  lazy val FULL_ISO8601_FORMAT = ISODateTimeFormat.dateTime

  implicit val DateTimeAsISO8601EncodeJson: EncodeJson[DateTime] =
    EncodeJson(s => jString(s.toString(FULL_ISO8601_FORMAT)))

  implicit val DateTimeAsISO8601DecodeJson: DecodeJson[DateTime] =
    implicitly[DecodeJson[String]].map(FULL_ISO8601_FORMAT.parseDateTime) setName "org.joda.time.DateTime"

  implicit def codec = derive[DAGResponse]
}

case class PaginatedDAGResponse(data: Seq[DAGResponse], hasMore: Boolean, nextFrom: Option[String])

object PaginatedDAGResponse {
  implicit def codec = derive[PaginatedDAGResponse]
}
