package kafkatutorial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args){

        String bootstrapServers="127.0.0.1:9092";
        System.out.println("Hello world");
        //create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // create Producer record
        ProducerRecord record = new ProducerRecord<String,String>("first_topic","Hello World");
        // send data - asynchrnous
        producer.send(record);

        //flush data
        producer.flush();

        producer.close();


        //
    }
}
