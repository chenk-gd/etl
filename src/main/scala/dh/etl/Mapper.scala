package dh.etl

import scala.io.Source
import scala.xml.Node
import scala.language.postfixOps

/**
  * Created by Administrator on 2015/5/26.
  */
trait Mapper {
  def get(key: String): String
  def keys: Iterable[String]
  def values: Iterable[String]
}

class NullMapper extends  Mapper {
  def get(key: String): String = key
  def keys : Iterable[String] = None
  def values : Iterable[String] = None
}

class FileMapper(file: String) extends Mapper {
  val m: Map[String, String] = Source.fromFile(file).getLines().toList.map(line => {
    val x = line.split("\\s")
    (x(0), x(1))
  }).toMap

  def get(key: String): String = m.getOrElse(key, "na")
  def keys : Iterable[String] = m.keys
  def values : Iterable[String] = m.values
}

object MapperFactory {
  def create(node: Node): Mapper = node \ "@type" text match {
    case "file" => new FileMapper(node \\ "file" text)
    case _  =>  new NullMapper
  }
}