package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

public class Customer {
    private static final String TOPIC="rui";
    private static final String BrokerList="localhost:9092";
    private static KafkaConsumer<String,String> consumer=null;
    private static ObjectMapper mapper=new ObjectMapper();
    private static Properties intiConfig(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BrokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        return properties;
    }
   static{
       Properties properties = intiConfig();
       consumer=new KafkaConsumer<>(properties);
       consumer.subscribe(Arrays.asList(TOPIC));
   }
    public static void main(String[] args) throws IOException {
        while(true){
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String,String> r:poll){
                List list = mapper.readValue(r.value(), List.class);
                LinkedHashMap<String, String> map = (LinkedHashMap<String, String>) list.get(0);
                for(String key:map.keySet()){
                    System.out.print(key+"'s value is "+map.get(key)+"\t");
                }
                System.out.println();
            }
        }
    }
}
