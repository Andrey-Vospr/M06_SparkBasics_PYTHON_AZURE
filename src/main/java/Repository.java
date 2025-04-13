package com.epam.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Class for reading and writing data in CSV or Parquet
 */
public class Repository implements Serializable {

    /**
     * Reads CSV file from the given path
     * @param session initialized Spark session
     * @param path full wasbs:// or local path to CSV file
     * @return dataset of rows
     */
    public Dataset<Row> readCSV(SparkSession session, String path) {
        System.out.println("Reading CSV from path: " + path);
        return session.read()
                      .option("header", "true")
                      .csv(path);
    }

    /**
     * Reads Parquet file from the given path
     * @param session initialized Spark session
     * @param path full wasbs:// or local path to Parquet file
     * @return dataset of rows
     */
    public Dataset<Row> readParquet(SparkSession session, String path) {
        System.out.println("Reading Parquet from path: " + path);
        return session.read().parquet(path);
    }

    /**
     * Writes Parquet file to the given path
     * @param ds dataset to write
     * @param path destination path
     * @param partitionBy optional partitioning columns
     */
    public void writeParquet(Dataset<Row> ds, String path, String... partitionBy) {
        System.out.println("Writing Parquet to path: " + path);
        ds.write()
          .partitionBy(partitionBy)
          .mode("overwrite")
          .parquet(path);
    }
}
