package com.capitalone.confluent.kafka.avro.producer.interfaces;

import java.util.Properties;

public class ProducerInterface {

    @FunctionalInterface
    public interface AvroSchema {
        public String getAvroSchema(String file);
    }

    @FunctionalInterface
    public interface ProducerProperties {
        public Properties getProducerProperties(String propertiFileName);
    }

    @FunctionalInterface
    public interface ProduceToTopic {
        public void produceAvroToTopic(String avroSchema, Properties properties , Long events);
    }

}
