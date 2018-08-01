package com.capitalone.avro.consumer;

import com.capitalone.avro.consumer.service.ConsumerInterfacesImpl;

public class Main {
    public static void main(String[] args) {

       String topic = args[0];
       int threads = Integer.parseInt(args[1]);

        ConsumerInterfacesImpl.avroConsumerRun.consumerRun(threads,topic);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }

        ConsumerInterfacesImpl.avroConsumerShutdown.consumerShutdown();
    }
}
