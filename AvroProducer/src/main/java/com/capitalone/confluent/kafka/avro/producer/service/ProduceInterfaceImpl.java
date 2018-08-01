package com.capitalone.confluent.kafka.avro.producer.service;

import com.capitalone.confluent.kafka.avro.producer.interfaces.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import java.util.Properties;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;


public class ProduceInterfaceImpl extends ProducerInterface {

    public static AvroSchema avroSchema = (filepath) -> {

        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines(Paths.get(filepath), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return contentBuilder.toString();
    };

    public static ProducerProperties producerProperties = (fileName) -> {

        Properties prop = new Properties();
        InputStream input = null;

        try{

            input = ProduceInterfaceImpl.class.getClassLoader().getResourceAsStream(fileName);
            if(input==null){
                System.out.println("Sorry, unable to find " + fileName);
                return null;
            }
            else
            {
                prop.load(input);

            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        return prop;
    };

    public static ProduceToTopic produceToTopic = (avroSchema, properties , events) -> {

        Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(properties);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(avroSchema);
        Random rnd = new Random();

        for (long nEvents = 0; nEvents < events; nEvents++) {
            long runtime = new Date().getTime();
            String site = "www.example.com";
            String ip = "192.168.2." + rnd.nextInt(255);

            GenericRecord page_visit = new GenericData.Record(schema);
            page_visit.put("time", runtime);
            page_visit.put("site", site);
            page_visit.put("ip", ip);

            ProducerRecord<String, GenericRecord> data = new ProducerRecord<String, GenericRecord>(
                    "page_visits", ip, page_visit);
            producer.send(data);
        }

        producer.close();
        System.out.println("Avro Data Inserted");

    };
}
