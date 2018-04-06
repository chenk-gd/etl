package dh.etl.transform

import dh.etl.Transformer

/**
  * Created by Administrator on 2015/6/1.
  */
class StringTrim extends Transformer{
  def transform(items: Array[_]) : List[String] = {
    items.map(_.toString.trim).toList
  }
}
