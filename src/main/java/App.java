package com.epam.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args) {

        // Load properties from conf.properties using PropertiesService
        PropertiesService propertiesService = new PropertiesService();

        // Read storage account name and key from config
        String storageAccountName = propertiesService.getProperty("azure.storage.account.name");
        String storageAccountKey = propertiesService.getProperty("fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net");

        // Set Spark and Hadoop configs dynamically using the storage account name
        SparkConf conf = new SparkConf()
                .set("fs.azure.account.auth.type." + storageAccountName + ".dfs.core.windows.net", "SharedKey")
                .set("fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net", storageAccountKey)
                .set("fs.azure.createRemoteFileSystemDuringInitialization", "true");

        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .master(propertiesService.getProperty("spark.master"))
                .appName(propertiesService.getProperty("spark.app.name"))
                .config(conf)
                .getOrCreate();

        // Load hotel and weather datasets
        Repository repo = new Repository();
        Dataset<Row> hotels = repo.readCSV(spark, propertiesService.getProperty("input.path.hotels"));
        Dataset<Row> weather = repo.readParquet(spark, propertiesService.getProperty("input.path.weather"));

        // Enrich and join data
        GeoService geoService = new GeoService();
        Transformer transformer = new Transformer(spark, geoService);
        Dataset<Row> hotelsWithCoords = transformer.addCoordinatesToHotels(hotels);
        Dataset<Row> enrichedHotels = transformer.addGeohashToHotels(hotelsWithCoords);
        Dataset<Row> enrichedWeather = transformer.addGeohashToWeather(weather);
        Dataset<Row> joined = transformer.joinByGeohash(enrichedHotels, enrichedWeather);

        // Write final result in partitioned Parquet format
        repo.writeParquet(joined, propertiesService.getProperty("output.path.result"), "year", "month", "day");

        spark.stop();
    }
}
