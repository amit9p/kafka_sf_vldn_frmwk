package com.capitalone.kafka.spark.sql.service;

import com.capitalone.kafka.spark.sql.Interface.SparkInterfaces;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkInterfacesImpl extends SparkInterfaces {

    public static Spark spark = (master) -> {
        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        SparkSession session = SparkSession.builder().getOrCreate();
        return session;
    };

    public static Dataframe dataframe = (spark, file) -> {

        Dataset<Row> dataset = SparkInterfacesImpl.spark.getSparkSession("local[*]").read().json(file);

        return dataset;
    };

    public static Table table = (dataset, tblnm) -> {

        dataset.createOrReplaceTempView(tblnm);

    };

    public static Query query = (spark, sql) -> {
        Dataset<Row> dataset = spark.sql(sql);

        return dataset;
    };

    public static Output output = (dataset , filePath) -> {

        dataset.write().csv(filePath);
    };
}
