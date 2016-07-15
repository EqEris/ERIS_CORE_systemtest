package com.eris

import scala.io.Source

object Templates {

  case class DAGFromTemplate(templateFile:String, replacements:Map[String, String]=Map.empty) {
    private val path =  getClass.getResource(s"/flows/$templateFile").getPath
    private val template = Source.fromFile(path).mkString

    val string = replacements.keys.foldLeft(template)((template: String, key: String) => {
      template.replace(key, replacements(key))
    })
  }

  case class MetricsFromTemplate(templateFile:String, replacements:Map[String, String]=Map.empty) {
    private val path =  getClass.getResource(s"/flows/$templateFile").getPath
    private val template = Source.fromFile(path).mkString

    val string = replacements.keys.foldLeft(template)((template: String, key: String) => {
      template.replace(key, replacements(key))
    })
  }
}
