package dh.etl.transform

import dh.etl.Transformer

/**
  * Created by Administrator on 2015/6/1.
  */
class DateTransform extends Transformer {
  def transform(date: String): List[String]  = {
    date.split('-').map(_.trim).toList
  }
}
