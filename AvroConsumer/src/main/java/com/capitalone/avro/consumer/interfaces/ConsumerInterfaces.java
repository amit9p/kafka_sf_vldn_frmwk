package com.capitalone.avro.consumer.interfaces;

import java.util.Properties;

public class ConsumerInterfaces {

    @FunctionalInterface
    public interface AvroConsumerProperty {
        public Properties getConsumerProperty(String fileName);
    }

    @FunctionalInterface
    public interface AvroConsumerRun {
        public void consumerRun(int threads , String topic);
    }

    @FunctionalInterface
    public interface AvroConsumerShutdown {
        public void consumerShutdown();
    }
}
