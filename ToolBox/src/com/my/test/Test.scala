package com.my.test

object Test {
  def main(arg: Array[String]) : Unit = {
  	//test
  	//val greet : Array[String] = new Array(3)
/*  	val greet = Array("Hello", "World", "Greeting!")
  	greet(0) = "hi"
  	greet(1) = "greeting"
  	greet(2) = "World!"
  	
  	greet.foreach (println)
  	greet.update(2, "Scala!")
  	//greet.foreach (println)
  	for( i <- 0 to 2) println (greet.apply(i))
 */ 	
  	val lst= List(("A",3), ("b",4), ("b",5), ("d",1), ("A",45))
  	lst foreach println
  	
  	val m = lst groupBy (ele => ele._1)
  	m foreach println

  	val r = m.map(x => (x._1, (x._2.map(y => y._2).sum)/x._2.size))
  	r foreach println
  	
  	for( i <- -1 to 1) println( i)
  	
  }
  def grp(ele: (String, Int)): String = {
    ele._1
  }
  def test = {
    var i = 0
    do {
      i = i + 1
      println("in loop")
    } while (i < 3)
    for (j <- 1 until 3; k <- "abc") println(j + " " + k)
    (1 until 3) foreach (i => "abc" foreach (j => println(i + " " + j)))

  }
}