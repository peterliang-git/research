package com.my.common

import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

abstract class HBaseTable [T: ClassTag](quorum: String, port: String, tableName: String) extends Loadable[T] with Serializable {

  
  def load(ss:SparkSession): RDD[T] = {
    //configuration
    val hbc = HBaseConfiguration.create()
    hbc.set("hbase.zookeeper.property.clientPort", port)
    hbc.set("hbase.zookeeper.quorum", quorum)
    hbc.set("zookeeper.znode.parent", "/hbase-unsecure")

    //set the table name
    hbc.set(TableInputFormat.INPUT_TABLE, tableName)
    //load the events
    val rowsRDD = ss.sparkContext.newAPIHadoopRDD(hbc, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    //parse
    rowsRDD.map({ case(k, v) => parse(k, v) })
  }

  //parse a row
  protected def parse(k:ImmutableBytesWritable, v:Result): T
}
