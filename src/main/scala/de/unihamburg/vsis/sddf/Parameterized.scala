package de.unihamburg.vsis.sddf

trait Parameterized {

  val name: String
  val paramMap: Map[String, Any]

  def getParameterString: String = {
    var result = name
    var paramString = ""
    paramMap.foreach(pair => {
      paramString += pair._1 + "=" + pair._2.toString + ", "
    })
    if(paramString.length > 0){
      result += "(" + paramString + ")"
    }
    result
  }

}