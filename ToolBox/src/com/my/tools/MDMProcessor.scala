package com.my.tools

import java.io.FileWriter
import java.io.BufferedWriter
import java.io.File
import scala.io.Source

object MDMProcessor {
  val inputDirNm: String = "C:/repository/loadsmall.log"
  val resultFileNm: String = "C:/repository/res.txt"

    def modifierFunc7(word: String): String = {
    val tw=word.trim()
    if(tw.startsWith("|") && tw.endsWith("|")) 
      tw.drop(1).dropRight(1) 
    else if(tw.endsWith("|")) 
      tw.dropRight(1)
    else if(tw.startsWith("|"))
        tw.drop(1)
    else
      tw
  }
  
  def modifierFunc8(word: String): String = if(word.trim().isEmpty()||word.trim().equals("-")) "" else word.trim()

  def main(arg: Array[String]) : Unit = {
    val starttime = System.currentTimeMillis()
    println("Input: " + inputDirNm + " Output: " + resultFileNm)
    val input = Array("737851481938824467","2018-01-01-10.09.48.15400","3000001","737851481938824467","1970-12-09-00.00.00.000000","RXMDHYHX J","EFSWNPF","1017721202","064 LSKHMX YDSE XPM426","TORONTO","M3G6Z4","108","31")
    val dr = new File(inputDirNm)

    val fw = new File(resultFileNm)

    val bw = new BufferedWriter(new FileWriter(fw))

    val fileList: Array[File] = if (dr.isDirectory()) dr.listFiles() else Array(dr)

    try {
      for (file <- fileList) {
        println("Processing " + file.getName)
        val t = readFile2(file, modifierFunc8) //read line, and do std
        /*
        for(ele <- t){
          println(ele.length + "=" + ele.mkString)
          //ele foreach println
        }
        */
        val m = t groupBy grp //reduce by grp key
        for(e <- m){
          println(e._1)
          for(v <- e._2){
            println(v.mkString)
          }
          println()
        }
        
        val cand = search(input,m) //find candidates by grp key
        for(e <- cand;
        ele <- e){
          println(ele.mkString)
        }
        matcher(input,cand) // score each record in candidates to the input
        
      }
    } finally {
      bw.close()
    }

    println("done in(ms) " + (System.currentTimeMillis() - starttime))  	
  	
  }
  
  def readFile(file: File, modifier: String => String): List[Array[String]] = {
    val source = Source.fromFile(file, "ISO-8859-1")
    
    try {
      val g = source.getLines
      val ll = for{ 
          line <- g
          if ! line.isEmpty()
          val sp = line.split(",") map (modifier)
      }yield sp
      
      val res = ll.toList
      
      res

    } finally {
      source.close()
    }
    
  }    
  def readFile2(file: File, modifier: String => String): List[Array[String]] = {
    val source = Source.fromFile(file, "ISO-8859-1")
    
    try {
      val g = source.getLines
      val ll = for{ 
          line <- g
          if ! line.isEmpty()
          val sp = line.split(",")
      }yield sp
      
      val finalrest = for{
        li <- ll
        val rest = li  flatMap (_.split("-\\s+")) map (modifier)
      }yield rest
      

      finalrest.toList
      
    } finally {
      source.close()
    }
    
  }  
  
  def search(line: Array[String], target: Map[String, List[Array[String]]]): List[List[Array[String]]] = {
    val key = grp(line)
    val res = for( e <- target;  if(e._1.equalsIgnoreCase(key)))yield e._2
    res.toList
  }
  def matcher(line: Array[String], target: List[List[Array[String]]]): List[List[Array[String]]] = {
    
    val inp = scoreableElements(line)
    
    println("input is "+inp)
    
    for(t <- target; e <- t) {
      val cmp = scoreableElements(e)
      println("compare input with "+ cmp.mkString)
      println("Score is " + scoreRecord(inp,cmp))
    }
    Nil
  }

    def scoreElement(s1: String, s2: String): Int={
      if(s1.equals(s2)) (1) else (0)
    }
    
  def scoreRecord(line1: List[String], line2: List[String]): Int = {
    assert(line1.length == line2.length)
    line1 match {
      case x::xs => scoreElement(x,line2.head) + scoreRecord(xs,line2.tail)
      case Nil => 0
    }
  }

  def scoreableElements(line: Array[String]): List[String] = {
    //SIN    //line(7)
    //LN+AddrOne     //line6 + line 8
    List(line(6),line(7),line(8))
  }
  //def f = {}
  def grp(line: Array[String]): String = {
    //SIN    //line(7)
    //LN+AddrOne     //line 6 + line 8
    line(6)+line(8)
  }  
}
