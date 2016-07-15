package com

import argonaut._, argonaut.DecodeJson._

package object eris {

  object Environments {
    val prod = "prod"
    val test = "test"
    val staging = "staging"
  }


  case class Error(nodeId: String, error: String)
  object Error {
    implicit val codec = derive[Error]
  }

  case class Errors(errors: List[Error])
  object Errors {
    implicit val codec = derive[Errors]
  }

  sealed trait Interval
  case object Seconds extends Interval
  case object Minutes extends Interval
  case object Hours extends Interval
  case object Days extends Interval


}
