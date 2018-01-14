package com.my.htable

import com.my.common.HBaseTable
import com.my.data.MDMCustomer
import com.my.data.MDMCustomer._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

class MDMCustomerTable (quorum: String, port: String) extends HBaseTable[MDMCustomer](quorum, port, "mdmcustomer") with Serializable {
  def this() = this("localhost.localdomain", "2181")

  //column families
  val cfProfile = Bytes.toBytes("profile")
  val cfAddress = Bytes.toBytes("address")

  //columns
  val cFirstname = Bytes.toBytes(nfirstname)
  val cLastname = Bytes.toBytes(nlastname)
  val cRefnum = Bytes.toBytes(nrefnum)
  val cAdminsys_id = Bytes.toBytes(nadminsys_id)
  val cAdminsys_tp = Bytes.toBytes(nadminsys_tp)
  val cBirth_dt = Bytes.toBytes(nbirth_dt)
  val cCreate_dt = Bytes.toBytes(ncreate_dt)
  val cAddr_lineone = Bytes.toBytes(naddr_lineone)
  val cCityname = Bytes.toBytes(ncityname)
  val cCountry = Bytes.toBytes(ncountry)
  val cState = Bytes.toBytes(nstate)
  val cZip = Bytes.toBytes(nzip)
  
  //parse a row
  override protected def parse(k:ImmutableBytesWritable, v:Result): MDMCustomer = {
    //set
    val user_id = Bytes.toString(k.copyBytes())                                   
    val firstname = Bytes.toString(v.getValue(cfProfile, cFirstname))             
    val lastname = Bytes.toString(v.getValue(cfProfile, cLastname))               
    val refnum = Bytes.toString(v.getValue(cfProfile, cRefnum))                     
    val adminsys_id = Bytes.toString(v.getValue(cfProfile, cAdminsys_id))            
    val adminsys_tp = Bytes.toString(v.getValue(cfProfile, cAdminsys_tp))             
    val birth_dt = Bytes.toString(v.getValue(cfProfile, cAdminsys_tp))                 
    val create_dt = Bytes.toString(v.getValue(cfProfile, cAdminsys_tp))                 

    val addr_lineone = Bytes.toString(v.getValue(cfAddress, cAddr_lineone))           
    val cityname = Bytes.toString(v.getValue(cfAddress, cCityname))          
    val country = Bytes.toString(v.getValue(cfAddress, cCountry))     
    val state = Bytes.toString(v.getValue(cfAddress, cState))         
    val zip = Bytes.toString(v.getValue(cfAddress, cZip))           
    //return
    MDMCustomer(user_id, firstname, lastname, refnum, adminsys_id, adminsys_tp, birth_dt, create_dt, 
        addr_lineone, cityname, country, state, zip)
  }
  
}