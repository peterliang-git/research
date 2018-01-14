package com.my.csvfile

import com.my.common.CsvFile
import com.my.data.MDMCustomer

class CSVFile (fileName: String) extends CsvFile[MDMCustomer](fileName) with Serializable {
  //def this() = this(fileName)

  	                //check if it is header
/*	               
if ( elements[0].equals("user_id") && elements[1].equals("create_dt") && elements[2].equals("adminsys_tp")&& elements[3].equals("adminsys_id") 
    && elements[4].equals("birth_dt") && elements[5].equals("firstname") && elements[6].equals("lastname")&& elements[7].equals("refnum")
  && elements[8].equals("addr_lineone") && elements[9].equals("cityname")&& elements[10].equals("zip")
&& elements[11].equals("state") && elements[12].equals("country") ) {
                    	                   //flag

case class MDMCustomer(user_id: String, firstname: String, lastname: String, refnum: String, adminsys_id: String, adminsys_tp: String, 
     birth_dt: String, create_dt: String, addr_lineone: String, cityname: String, country: String, state: String, zip: String){

* */
  
  protected override def parse(s: Array[String]): MDMCustomer = {
    MDMCustomer(s(0), s(5), s(6), s(7), s(3), s(2), s(4), s(1), s(8), s(9), s(12), s(11), s(10))
  }
}
