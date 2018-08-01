package com.capitalone.kafka.spark.sql.main;

import com.capitalone.kafka.spark.sql.service.SparkInterfacesImpl;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {

        String inputFile = "/home/amitprasad/MyFolder/Tech/confluent_kafka/avro.json";
        String tableNm = "people";
        String sql = "select * from people";
        String outputFile = "/home/amitprasad/MyFolder/Tech/confluent_kafka/avro.csv";

        SparkSession spark = SparkInterfacesImpl.spark.getSparkSession("local[*]");

        Dataset<Row> people = SparkInterfacesImpl.dataframe.getDataFrame(spark, inputFile);

        SparkInterfacesImpl.table.createTable(people, tableNm);

        Dataset<Row> result = SparkInterfacesImpl.query.executeQuery(spark, sql);


        SparkInterfacesImpl.output.writeQueryOutput(result, outputFile);


    }
}
