package com.my.tools

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

import scala.io.Source

object LogProcessor {
  
  val inputDirNm: String = "C:/repository/loadsmall.log"
  val resultFileNm: String = "C:/repository/res.txt"
  val ext: String = ".log"

  def filterFunc1(line: String): Boolean = (line.contains("Total execution time (sec.microsec)=") && line.substring(37, line.length()).toDouble > 20.0 ) // ||line.contains("Number of executions") || line.contains("Statement text")
  def modifierFunc1(line: String): String = line

  def filterFunc2(line: String): Boolean = !(line.trim().startsWith("TEMP_") || line.trim().startsWith("SIB") || line.trim().startsWith("V_") || line.trim().startsWith("CD") || line.trim().startsWith("H_") || line.trim().isEmpty())
  def modifierFunc2(line: String): String = "select count(*) from " + line.split("     ").head
  
  def filterFunc3(line: String): Boolean = line.contains("searchSuspectsByObject")

  def filterFunc4(line: String): Boolean = line.trim().startsWith("[")


  def filterFunc5(line: String): Boolean = line.trim().startsWith("<ErrorMessage>") || line.trim().startsWith("</TCRMService>,")
  def modifierFunc5(line: String): String = line.trim()

  def filterFunc6(line: String): Boolean = line.trim().contains("BTM ITxRxException occurred in Request and Response Framework")
  def modifierFunc6(line: String): String = line.substring(line.indexOf("<AdminPartyId>"), line.lastIndexOf("</AdminSystemType>")+18)

  
  def process(file: File, bw: BufferedWriter, matcher: String => Boolean, modifier: String => String): Unit = {
    val source = Source.fromFile(file, "ISO-8859-1")
    try {
      bw.newLine()
      bw.write("Processing " + file.getName)
      bw.newLine()
      for (line <- (source.getLines withFilter (matcher(_)))) {
        bw.write(modifier(line))
        bw.newLine()
      }
    } finally {
      source.close()
    }
  }  
  def dupleProcess(file: File, bw: BufferedWriter, matcher: String => Boolean, modifier: String => String): Unit = {
    val source = Source.fromFile(file, "ISO-8859-1")
    try {
      for( line <- pairTravers(source.getLines.toList, matcher)) {
          bw.write(modifier(line))
          bw.newLine()
      }
    } finally {
      source.close()
    }
  }  

  def pairTravers(strLs : List[String], matcher: String => Boolean) : List[String] = {
    strLs match {
      case x :: xs => if(matcher(x)) xs.head :: pairTravers(xs.tail, matcher) else pairTravers(xs, matcher)
      case _ => Nil
    }
  }
    
  def inMemProcess2 (file: File, matcher: String => Boolean, modifier: String => String): Unit = {
    val source = Source.fromFile(file, "ISO-8859-1")
    try {
      for( v <- pairTravers(source.getLines.toList, matcher)) println (modifier(v))
    } finally {
      source.close()
    }
  }  
    
  def inMemProcess(file: File): Unit = {
    val source = Source.fromFile(file, "ISO-8859-1")
    
    try {
      val g = source.getLines.grouped(2).toList
      for( u <- g ) {
        u match {
          case "<ErrorMessage>Maximum number of the following is exceeded: IdentificationType</ErrorMessage>" :: a => println(modifierFunc6(a(0)))
          case _ =>
        }
      }
      //g foreach println
      val r = g filter (_(0).contains("Maximum number of the following is exceeded: IdentificationType") )
      for( v <- r) println (modifierFunc6(v(1)))

    } finally {
      source.close()
    }
  }  
  
  def main(arg: Array[String]) : Unit = {
    val starttime = System.currentTimeMillis()
    println("Input: " + inputDirNm + " Output: " + resultFileNm)
    val dr = new File(inputDirNm)
    val fw = new File(resultFileNm)
    val bw = new BufferedWriter(new FileWriter(fw))
    val fileList: Array[File] = if (dr.isDirectory()) dr.listFiles() else Array(dr)

    try {
      for (file <- fileList if file.getName.toLowerCase().endsWith(ext)) {
        println("Processing " + file.getName)
        process(file, bw, filterFunc1, modifierFunc1)
        //dupleProcess(file, bw, filterFunc6, modifierFunc6)
        //inMemProcess2(file,filterFunc6, modifierFunc6)
      }
    } finally {
      bw.close()
    }

    println("done in(ms) " + (System.currentTimeMillis() - starttime))
  }

}