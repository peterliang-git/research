package com.my.tools

import com.my.htable.MDMCustomerTable
import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import com.my.data.MDMCustomer

object Loader {
  val mdmcustomertbl : MDMCustomerTable = new MDMCustomerTable("localhost.localdomain", "2181")
  val appName: String = "MDMSparkLoader"
    
  def main(arg: Array[String]) : Unit = {
    
    //create the spark context
    val ss = SparkSession.builder.appName(appName).getOrCreate()
    try {
      //flat starting
      println("Starting ...")

      val cusRDD = mdmcustomertbl.load( ss )
//user_id: String, firstname: String, lastname: String, refnum: String, adminsys_id: String, adminsys_tp: String, 
  //   birth_dt: String, create_dt: String, addr_lineone: String, cityname: String, country: String, state: String, zip: String){
        
      val cusDF = ss.createDataFrame(
          cusRDD.map(t => org.apache.spark.sql.Row(t.user_id,t.firstname,t.lastname,t.refnum,t.adminsys_id,
          t.adminsys_tp,t.birth_dt,t.create_dt,t.addr_lineone,t.cityname,t.country,t.state,t.zip)), 
          MDMCustomer.getSchema())

      cusDF.createOrReplaceTempView("customer")
      
      //save
      cusDF.write.format("csv").mode("overwrite").option("header", "true").save("/user/peter/mdm")
      //print
      ss.sql("SELECT user_id, firstname, lastname FROM customer limit 10").show()    

    }
    finally {
      //stop
      ss.stop()
      //flag done
      println("Done!!!")
    }
  	
  }
}