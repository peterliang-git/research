package com.my.test

import java.io.File
import java.util.HashMap

import scala.io.Source
import scala.xml.MetaData
import scala.xml.pull.EvComment
import scala.xml.pull.EvElemEnd
import scala.xml.pull.EvElemStart
import scala.xml.pull.EvText

object Exec {
  def main(arg: Array[String]) : Unit = {
  	  val s = """user_012342qd	2017-10-12 1.432534523	2.123123125 rt@user_12341232 this is a test@user_2321e342 aba
   user_012342qd	2017-10-12 1.432534523	2.123123125 rt@user_12341333 this is a test@user_2321e444"""
  	  val p = "(@user_[a-zA-Z0-9]+)".r 
  	  p.findAllIn(s).foreach(println)
  	  val res = s.split('\n').map(x=>'@'+x.trim()).map(y=>p.findAllIn(y).toList)
  	  val res1 = res.flatMap(x=>x.tail.map(y=>(x.head, y)))
  	  for(a <- res1){
  	    println("got one: "+a)
  	  }
 	 
  	 //val xmlBooks= scala.xml.XML.loadFile("c:/repository/sample.xml")
  	 //println (xmlBooks)
  	  //val is = new java.io.FileInputStream("c:/repository/sample.xml")
  	  
  	 val fn = "c:/repository/sample.xml"
  	 val is = Source.fromFile(new File(fn),"UTF-8")
  	 
/*
    try {
      println("Processing " + fn)
      val xmlReader = scala.xml.XML.parser.getXMLReader
      for (line <- (is.getLines withFilter (!_.trim().isEmpty()))) {
        //xmlReader.(line.trim)
        //println(x.label +":"+x.text)
      }
    } finally {
      is.close()
    }	 
*/  	 
 	 
    	val xml = new scala.xml.pull.XMLEventReader(is)
      val m : HashMap[String,String] = new HashMap()
      //val xpath : ListBuffer[String] = ListBuffer()
     var xpath : List[String] = List()
 
  	 var cont:String = ""
      try {
        for ( e <- xml) {
          //for ( e <-new XMLEventReader(scala.io.Source.fromInputStream(new BOMInputStream(is)) )) {
          e match {
              case EvElemStart(_, tag, attrs, _) => {
                xpath =  xpath.::(tag)
                if ( attrs!= null ){
                  val k = listToString(xpath.reverse, "/")
                  attrs.foreach( (x:MetaData) => m.put(k+"#"+x.key, x.value.toString) )
                }
              }
              case EvText(text) => { cont = text.toString()  }
              case EvElemEnd(_, tag) => {
                val xp = listToString(xpath.reverse, "/")
                m.put(xp, cont)
                xpath = xpath.tail
                cont = ""
              }
              case EvComment(text) => {println("comment: "+text)}
          }
        }
      } finally {
        is.close()
      }
      val q1 = "/TCRMPartyAddressBObj/TCRMAddressBObj/AddressLineOne"
      val q2 = "/TCRMPartyAddressBObj/AddressUsageValue#primary"
      println(m.get(q1))
      println(m.get(q2))
  }
  
  def listToString(list : List[String], s: String): String = {
    if(list.isEmpty) ""
    else s + list.head + listToString(list.tail, s)
  }
}