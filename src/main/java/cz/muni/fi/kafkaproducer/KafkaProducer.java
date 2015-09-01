package cz.muni.fi.kafkaproducer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class KafkaProducer {
    public static void main(String[] args) {
        
    if(args.length < 1){
        throw new IllegalArgumentException("Missing inputFile argument");
    }
    
    String inputFile = args[0];   
    
    Properties kafkaProperties = PropertiesParser.getProperties();
    String topic = kafkaProperties.getProperty("kafkaProducer.topic");
    
    kafkaProperties.put("metadata.broker.list", kafkaProperties.getProperty("kafkaProducer.metadata.broker.list"));
    kafkaProperties.put("serializer.class", "kafka.serializer.StringEncoder");
    kafkaProperties.put("partitioner.class", "cz.muni.fi.kafkaproducer.RoundRobinCustomPartitioner");
    kafkaProperties.put("request.required.acks", "0");
    kafkaProperties.put("producer.type", "async");
    kafkaProperties.put("batch.size", kafkaProperties.getProperty("kafkaProducer.batchSize"));
    
    ProducerConfig config = new ProducerConfig(kafkaProperties);
    Producer<String, String> producer = new Producer<String, String>(config);
    
    Integer count = 0;
    try {
    BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
    while (true) {
        String line=reader.readLine();
        if (line == null)     
            break;
        count++;
        KeyedMessage<String,String> data=new KeyedMessage<String,String>(topic,line);
        producer.send(data);      
    }
    } catch (FileNotFoundException ex) {
        throw new IllegalArgumentException("Error: File not found.",ex);
    } catch (IOException ioex) {
        throw new RuntimeException("Error: Reading from file", ioex);
    }
  
    producer.close();
    System.out.println("Sent " + count.toString() + " messages to Kafka successfully");
    }
}

