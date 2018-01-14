package com.my.data

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

case class MDMCustomer(user_id: String, firstname: String, lastname: String, refnum: String, adminsys_id: String, adminsys_tp: String, 
     birth_dt: String, create_dt: String, addr_lineone: String, cityname: String, country: String, state: String, zip: String){
  
}

/*
 address:addr_lineone timestamp=1515022463049, value=#359 56 UNHKZHOOOZ PJHQ    
 address:cityname     timestamp=1515022463049, value=TORONTO                    
 address:country      timestamp=1515022463049, value=31                         
 address:state        timestamp=1515022463049, value=108                        
 address:zip          timestamp=1515022463049, value=M8E4B4                     
 profile:adminsys_id  timestamp=1515022463049, value=153151481949917281         
 profile:adminsys_tp  timestamp=1515022463049, value=3000001                    
 profile:birth_dt     timestamp=1515022463049, value=1970-12-02-00.00.00.000000 
 profile:create_dt    timestamp=1515022463049, value=2018-01-01-10.11.39.071000 
 profile:firstname    timestamp=1515022463049, value=INZQ                       
 profile:lastname     timestamp=1515022463049, value=TQNRNT                     
 profile:refnum       timestamp=1515022463049, value=101752123200  
*/

object MDMCustomer{
  //row key
  val nuser_id = "user_id"

  //columns
  val nfirstname = "firstname"
  val nlastname = "lastname"
  val nrefnum = "refnum"
  val nadminsys_id = "adminsys_id"
  val nadminsys_tp = "adminsys_tp"
  val nbirth_dt = "birth_dt"
  val ncreate_dt = "create_dt"
  val naddr_lineone = "addr_lineone"
  val ncityname = "cityname"
  val ncountry = "country"
  val nstate = "state"
  val nzip = "zip"
  
  //get the schema of the current entity
  def getSchema(): StructType = {
    //create struct type
    StructType(StructField(nuser_id, StringType, true)
      :: StructField(nfirstname, StringType, true)
      :: StructField(nlastname, StringType, true)
      :: StructField(nrefnum, StringType, true)
      :: StructField(nadminsys_id, StringType, true)
      :: StructField(nadminsys_tp, StringType, true)
      :: StructField(nbirth_dt, StringType, true)
      :: StructField(ncreate_dt, StringType, true)
      :: StructField(naddr_lineone, StringType, true)
      :: StructField(ncityname, StringType, true)
      :: StructField(ncountry, StringType, true)
      :: StructField(nstate, StringType, true)
      :: StructField(nzip, StringType, true) :: Nil)
  }
}