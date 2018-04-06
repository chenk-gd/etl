package dh.etl.transform

import dh.etl.Mapper

/**
  * Created by Administrator on 2015/5/27.
  */
class Mappers {
  def simpleMapper(s: String, m: Mapper) : String = {
    m.get(s)
  }

  def phoneNumber(phone: String, m: Mapper) : List[String] = {
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
