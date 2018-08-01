package com.capitalone.confluent.kafka.avro.producer.main;

import com.capitalone.confluent.kafka.avro.producer.service.ProduceInterfaceImpl;

public class Main {
    public static void main(String[] args) {

        Long events = Long.parseLong(args[0]);
        String schemaFile = args[1];

        //Calling Avro Producer to push desired number of events to Topic
        ProduceInterfaceImpl.produceToTopic.produceAvroToTopic(ProduceInterfaceImpl.avroSchema.getAvroSchema(schemaFile), ProduceInterfaceImpl.producerProperties.getProducerProperties("producer.properties"), events);


    }
}
