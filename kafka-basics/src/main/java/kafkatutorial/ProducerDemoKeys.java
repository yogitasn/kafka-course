package kafkatutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers="127.0.0.1:9092";
        System.out.println("Hello world");
        //create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the Producer
        final KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10; i++) {

            String topic = "first_topic";
            String value = "Hello World "+Integer.toString(i);
            String key = "id_" +Integer.toString(i);


            // create Producer record
            ProducerRecord record = new ProducerRecord<String, String>(topic,key,value);

            logger.info("Key "+key); //log the key
            // id_1 goes to partition 0
            // id_2 to partition 2
            // id_3 to partition 0
            // id_4 to partition 2
            // id_5 to partition 2
            // id_6 to partition 0
            // id_7 to partition 2
            // id_8 to partition 1
            // id_9 to partition 2

            // send data - asynchronous                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       s
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        logger.error("Error while producing ", e);
                    }

                }
            }).get(); // block the .send() to make it synchronous = DON'T DO this in PRODUCTION
        }

        //flush data
        producer.flush();

        producer.close();


        //
    }
}
