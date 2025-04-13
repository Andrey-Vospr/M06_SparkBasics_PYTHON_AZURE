package com.epam.spark;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SchemaTest {

    private SparkSession session;
    private Schema schemaUtil;

    private StructType hotelSchema;
    private StructType weatherSchema;
    private StructType joinedSchema;

    @BeforeAll
    public void setup() {
        session = SparkSession.builder().master("local[*]").appName("SchemaTest").getOrCreate();
        schemaUtil = new Schema();
        initSchemas();
    }

    private void initSchemas() {
        hotelSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("Id", DataTypes.StringType, false),
                DataTypes.createStructField("Name", DataTypes.StringType, false),
                DataTypes.createStructField("Country", DataTypes.StringType, false),
                DataTypes.createStructField("City", DataTypes.StringType, false),
                DataTypes.createStructField("Address", DataTypes.StringType, false),
                DataTypes.createStructField("Latitude", DataTypes.StringType, true),
                DataTypes.createStructField("Longitude", DataTypes.StringType, true),
                DataTypes.createStructField("geohash", DataTypes.StringType, false)
        });

        weatherSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("avg_tmpr_c", DataTypes.StringType, false),
                DataTypes.createStructField("avg_tmpr_f", DataTypes.StringType, false),
                DataTypes.createStructField("lat", DataTypes.DoubleType, false),
                DataTypes.createStructField("lng", DataTypes.DoubleType, false),
                DataTypes.createStructField("wthr_date", DataTypes.StringType, false),
                DataTypes.createStructField("year", DataTypes.StringType, false),
                DataTypes.createStructField("month", DataTypes.StringType, false),
                DataTypes.createStructField("day", DataTypes.StringType, false),
                DataTypes.createStructField("geohash", DataTypes.StringType, false)
        });

        joinedSchema = DataTypes.createStructType(
                JavaConverters.seqAsJavaList(schemaUtil.joinedSchema(weatherSchema, hotelSchema))
                        .stream()
                        .filter(field -> !field.name().equals("Latitude") && !field.name().equals("Longitude"))
                        .collect(Collectors.toList()));
    }

    @Test
    public void testGetHotelSchema() {
        StructType actual = schemaUtil.hotelSchema();
        assertEquals(hotelSchema, actual);
    }

    @Test
    public void testGetWeatherSchema() {
        StructType actual = schemaUtil.weatherSchema();
        assertEquals(weatherSchema, actual);
    }

    @Test
    public void testJoinedSchemaMerge() {
        StructType actual = DataTypes.createStructType(
                JavaConverters.seqAsJavaList(schemaUtil.joinedSchema(weatherSchema, hotelSchema))
                        .stream()
                        .filter(field -> !field.name().equals("Latitude") && !field.name().equals("Longitude"))
                        .collect(Collectors.toList()));
        assertEquals(joinedSchema, actual);
    }

    @Test
    public void testRowConstructionWithSchema() {
        Dataset<Row> hotels = initHotelRows(hotelSchema);
        assertEquals(2, hotels.count());
        assertEquals(hotelSchema, hotels.schema());
    }

    private Dataset<Row> initHotelRows(StructType schema) {
        return session.createDataset(Arrays.asList(
                RowFactory.create("1", "Sunset Inn", "US", "Tatum", "Main St", "32.31599", "-94.51659", "9vsz"),
                RowFactory.create("2", "Ihg", "US", "Metropolis", "5th St", "37.15117", "-88.732", "dn8g")
        ), RowEncoder.apply(schema));
    }

    @AfterAll
    public void tearDown() {
        session.stop();
    }
}
