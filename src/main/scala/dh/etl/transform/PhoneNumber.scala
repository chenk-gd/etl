package dh.etl.transform

import dh.etl.{Mapper, Transformer}

/**
  * Created by Administrator on 2015/6/1.
  */
class PhoneNumber extends Transformer{
  def transform(phone: String, m: Mapper): List[String] = {
    var areacode: String = null
    var number: String = null
    var city: String = null
    for(ac <- m.keys if phone.startsWith(ac)) {
      areacode = ac
      number = phone.substring(areacode.length)
      city = m.get(ac)
    }
    List(areacode, number, city)
  }
}
