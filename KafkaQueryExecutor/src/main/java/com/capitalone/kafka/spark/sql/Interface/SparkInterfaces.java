package com.capitalone.kafka.spark.sql.Interface;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkInterfaces {

    @FunctionalInterface
    public interface Spark {
        public SparkSession getSparkSession(String master);
    }

    @FunctionalInterface
    public interface Dataframe {
        public Dataset<Row> getDataFrame(SparkSession spark ,String file );
    }

    @FunctionalInterface
    public interface Table {
        public void createTable(Dataset<Row> dataset , String tableNm);
    }

    @FunctionalInterface
    public interface Query{
        public Dataset<Row> executeQuery(SparkSession spark , String query);
    }

    @FunctionalInterface
    public interface Output{
        public void writeQueryOutput(Dataset<Row> dataset  , String outputFile);
    }
}
