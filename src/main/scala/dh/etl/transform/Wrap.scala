package dh.etl.transform

import dh.etl.Transformer

/**
  * Created by Administrator on 2015/6/1.
  */
class Wrap extends Transformer{
  def transform(items: Array[_], delim: String) : List[String] = {
    items.map(x=>delim + x + delim).toList
  }
}
