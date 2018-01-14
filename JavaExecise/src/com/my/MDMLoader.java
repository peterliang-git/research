package com.my;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MDMLoader {
	
	private static String hostname = "localhost.localdomain";
	private static String hbasesite = "/usr/hdp/current/hbase-client/conf/hbase-site.xml";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	    if (args.length < 1) {
	        System.out.println("Usage: MDMLoader <topic>");
	        System.out.println("<topic>: the kafka topic created for mdm data");
	        return;
	      }

	      //topic name
	      String topic = args[0].toString();
	      //consumer group
	      String group = "grpMDM";    

	      //prepare properties for the consumer
	      Properties props = new Properties();
	      props.put("bootstrap.servers", hostname+":6667");
	      props.put("group.id", group);
	      props.put("enable.auto.commit", "false");
	      props.put("auto.commit.interval.ms", "1000");
	      props.put("request.timeout.ms", "180000");
	      props.put("session.timeout.ms", "120000");
	      props.put("max.poll.records", "256");
	      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	      //create consumer
	      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	      //subscribe
	      consumer.subscribe(Arrays.asList(topic));
	      //print message
	      System.out.println("Subscribed to topic -> " + topic);
	      try {
	        //loop for reading   
	        while ( true ) {
	          //poll records
	          ConsumerRecords<String, String> records = consumer.poll( 10000 );
	          System.out.println("consumerRecords poll count == " + records.count());
	          //check
	          if ( records != null && records.count() > 0 ) {
	            //check
	              //write to HBase
	              writeHBase( records );
	            //commit
	            consumer.commitSync();
	          }
	        }  
	      }
	      catch ( Exception e ) {
	        //print error
	        e.printStackTrace();
	      }
	      finally {
	        //close the consumer
	        consumer.close();
	      }
	    }
	  static byte[] profile = Bytes.toBytes("profile");
	  static byte[] address = Bytes.toBytes("address");

	    private static void writeHBase( ConsumerRecords<String, String> records ) throws Exception {
	    
	      //configuration
	      Configuration cfg = HBaseConfiguration.create();
	      //set resource
	      cfg.addResource( new Path(hbasesite) );
	      //establish a connection
	      Connection conn = ConnectionFactory.createConnection( cfg );
	      System.out.println("in writeHBase");
	      try {
	        //HTable
	        Table users = conn.getTable( TableName.valueOf("mdmcustomer") );
	        try {
	          long count = 0, passHead = 0;
	          //loop    
	          for ( ConsumerRecord<String, String> record : records ) {
	            //parse user record
	            String[] elements = record.value().split( "," );
	            System.out.println("elements length == "+ elements.length);
	            //check
	            if ( elements != null && elements.length == 13 ) {
	              //check if head is passed
	              if ( passHead == 0 ) {
	                //check if it is header
	                if ( elements[0].equals("user_id") && elements[1].equals("create_dt") && elements[2].equals("adminsys_tp")&& elements[3].equals("adminsys_id") 
	                        && elements[4].equals("birth_dt") && elements[5].equals("firstname") && elements[6].equals("lastname")&& elements[7].equals("refnum")
		                    && elements[8].equals("addr_lineone") && elements[9].equals("cityname")&& elements[10].equals("zip")
	                    && elements[11].equals("state") && elements[12].equals("country") ) {
	                    	                   //flag
	                   passHead = 1;
	                   //skip
	                   continue;
	                 }
	              }

	              //user id
	              Put p = new Put(Bytes.toBytes(elements[0].trim()));

	              //profile
	              p = p.addColumn(profile, Bytes.toBytes("create_dt"), Bytes.toBytes(elements[1].trim()));
	              p = p.addColumn(profile, Bytes.toBytes("adminsys_tp"), Bytes.toBytes(elements[2].trim()));
	              p = p.addColumn(profile, Bytes.toBytes("adminsys_id"), Bytes.toBytes(elements[3].trim()));
	              p = p.addColumn(profile, Bytes.toBytes("birth_dt"), Bytes.toBytes(elements[4].trim()));
	              p = p.addColumn(profile, Bytes.toBytes("firstname"), Bytes.toBytes(elements[5].trim()));
	              p = p.addColumn(profile, Bytes.toBytes("lastname"), Bytes.toBytes(elements[6].trim()));
	              p = p.addColumn(profile, Bytes.toBytes("refnum"), Bytes.toBytes(elements[7].trim()));

	              //address
	              p = p.addColumn(address, Bytes.toBytes("addr_lineone"), Bytes.toBytes(elements[8].trim()));
	              p = p.addColumn(address, Bytes.toBytes("cityname"), Bytes.toBytes(elements[9].trim()));
	              p = p.addColumn(address, Bytes.toBytes("zip"), Bytes.toBytes(elements[10].trim()));
	              p = p.addColumn(address, Bytes.toBytes("state"), Bytes.toBytes(elements[11].trim()));
	              p = p.addColumn(address, Bytes.toBytes("country"), Bytes.toBytes(elements[12].trim()));

	              //save
	              users.put( p );
	              //increase the counter
	              count++;
	            }
	          }

	          //print
	          System.out.printf("%d records persisted\n", count);
	        }
	        finally {
	          //close the table
	          users.close();
	        }
	      }
	      finally {
	        //close the connection
	        conn.close();
	      }
	    }

}
