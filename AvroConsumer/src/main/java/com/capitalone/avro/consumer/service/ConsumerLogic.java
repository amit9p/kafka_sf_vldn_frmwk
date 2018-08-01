package com.capitalone.avro.consumer.service;

import org.apache.avro.generic.GenericRecord;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;


import java.sql.SQLOutput;

public class ConsumerLogic implements Runnable {
    private KafkaStream stream;
    private int threadNumber;
    public static String data = "";

    public ConsumerLogic(KafkaStream stream, int threadNumber) {
        this.threadNumber = threadNumber;
        this.stream = stream;
    }

    public void run() {
        ConsumerIterator<Object, Object> it = stream.iterator();

        while (it.hasNext()) {
            MessageAndMetadata<Object, Object> record = it.next();

            String topic = record.topic();
            int partition = record.partition();
            long offset = record.offset();
            Object key = record.key();
            GenericRecord message = (GenericRecord) record.message();
            String displayMessage = message.toString();
            data = data.concat(displayMessage).concat("\n");
            System.out.println("Thread " + threadNumber +
                    " received: " + "Topic " + topic +
                    " Partition " + partition +
                    " Offset " + offset +
                    " Key " + key +
                    " Message " + displayMessage);
        }
        System.out.println("Shutting down Thread: " + threadNumber);
        System.out.println("########################");
        System.out.println(data);
        String outputFile = ConsumerInterfacesImpl.avroConsumerProperty.getConsumerProperty("Consumer.properties").getProperty("avro.consumer.data");

        writeUsingOutputStream(data, outputFile);
        System.out.println("File Write Completed");

    }

    private static void writeUsingOutputStream(String data, String filepath) {
        OutputStream os = null;
        try {
            os = new FileOutputStream(new File(filepath));
            os.write(data.getBytes(), 0, data.length());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
