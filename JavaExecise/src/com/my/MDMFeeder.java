package com.my;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MDMFeeder {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	    //check arguments
	    if (args.length != 3) {
	      System.out.println("broker-address topic file-name");
	    }
	    else {
	      //assign broker url
	      String brokerUrl = args[0].toString();
	      //assign topicName to string variable
	      String topicName = args[1].toString();
	      //assign file name
	      String fileName = args[2].toString();

	      //create an instance for properties to access the producer configs
	      Properties props = new Properties();
	      //assign localhost id
	      props.put("bootstrap.servers", brokerUrl);
	   
	      //set acknowlegements for producer requests
	      props.put("acks", "all");

	      //if the request fails, the producer can automatically retry
	      props.put("retries", 0);
	      //specify buffer size in config
	      props.put("batch.size", 16384);

	      //reduce the # of requests less than 0
	      props.put("linger.ms", 1);

	      //the buffer.memory controls the total amount of memory available to the producer
	      props.put("buffer.memory", 33554432);

	      //key serializer
	      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	      //value serializer
	      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

	      //create producer
	      Producer<String, String> producer = new KafkaProducer<String, String>(props);
	      try {
	        //file
	        BufferedReader br = new BufferedReader( new FileReader(new File(fileName)) );
	        try {
	            //position
	            long pos = 0, count = 0;
	            //text
	            String text = br.readLine();
	            //check
	            while ( text != null ) {
	              //adjust position
	              pos += text.length() + 2;
	            final ProducerRecord<String, String> record =new ProducerRecord<String, String>(topicName, Long.toString(pos), text);
	              //send
	              RecordMetadata metadata = producer.send( record ).get();
	              System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
	            		  record.key(), record.value(), metadata.partition(),
	              metadata.offset());

	              //count
	              count++;
	              //next read
	              text = br.readLine();
	            }

	            //print
	            System.out.println( Long.toString(count) + " messages sent to " + topicName);
	        }
	        finally {
	          //close the reader
	          br.close();
	        }
	      }
	      catch ( java.lang.Exception e ) {      
	        //print
	        e.printStackTrace();
	      }
	      finally {
	        //close
	    	  producer.flush();
	        producer.close();
	      }
	    }
	  }
}    
