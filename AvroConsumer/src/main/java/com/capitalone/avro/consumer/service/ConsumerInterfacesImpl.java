package com.capitalone.avro.consumer.service;

import com.capitalone.avro.consumer.interfaces.ConsumerInterfaces;

import java.io.InputStream;
import java.util.Properties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.VerifiableProperties;


public class ConsumerInterfacesImpl extends ConsumerInterfaces {

    public static ConsumerConnector consumer = null;
    public static ExecutorService executor = null;


    public static AvroConsumerProperty avroConsumerProperty = (fileName) -> {


        Properties prop = new Properties();
        InputStream input = null;

        try {

            input = ConsumerInterfacesImpl.class.getClassLoader().getResourceAsStream(fileName);
            if (input == null) {
                System.out.println("Sorry, unable to find " + fileName);
                return null;
            } else {
                prop.load(input);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return prop;
    };

    public static AvroConsumerRun avroConsumerRun = (threads, topic) -> {

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                new ConsumerConfig(ConsumerInterfacesImpl.avroConsumerProperty.getConsumerProperty("Consumer.properties")));

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, threads);
        VerifiableProperties vProps = new VerifiableProperties(ConsumerInterfacesImpl.avroConsumerProperty.getConsumerProperty("Consumer.properties"));

        // Create decoders for key and value
        KafkaAvroDecoder avroDecoder = new KafkaAvroDecoder(vProps);
        Map<String, List<KafkaStream<Object, Object>>> consumerMap =
                consumer.createMessageStreams(topicCountMap, avroDecoder, avroDecoder);
        List<KafkaStream<Object, Object>> streams = consumerMap.get(topic);

        // Launch all the threads
        executor = Executors.newFixedThreadPool(threads);
        // Create ConsumerLogic objects and bind them to threads
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerLogic(stream, threadNumber));
            threadNumber++;
        }

    };

    public static AvroConsumerShutdown avroConsumerShutdown = () -> {

        if (ConsumerInterfacesImpl.consumer != null) ConsumerInterfacesImpl.consumer.shutdown();
        if (ConsumerInterfacesImpl.executor != null) ConsumerInterfacesImpl.executor.shutdown();
        try {
            if (!ConsumerInterfacesImpl.executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println(
                        "Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }

    };

}

