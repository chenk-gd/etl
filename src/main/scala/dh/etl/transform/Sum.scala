package dh.etl.transform

import dh.etl.Transformer

/**
  * Created by Administrator on 2015/6/1.
  */
class Sum extends Transformer{
  def transform(u: Double, d: Double) : Double = u + d
}
