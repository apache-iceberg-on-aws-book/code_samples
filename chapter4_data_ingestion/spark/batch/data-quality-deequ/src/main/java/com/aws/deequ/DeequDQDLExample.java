package com.aws.deequ;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.amazon.deequ.dqdl.EvaluateDataQuality;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;

public class DeequDQDLExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DeequDQDL-Java")
                .config("spark.master", "local[*]")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();


        // Read DQDL rules from JSON file
        String ruleset = readDQDLRulesAsString("dqdl_rules.txt");

        // Sample Data
        Dataset<Row> df = createSampleData(spark);
        System.out.println("Sample data created: " + df.count() + " records\n");
        df.show(5, false);
        // Evaluate data quality
        Dataset<Row> results = EvaluateDataQuality.process(df, ruleset);
        results.show();

        spark.stop();
    }


    private static String readDQDLRulesAsString(String filename) {
        try {
            return new String(Files.readAllBytes(Paths.get(filename)));
        } catch (IOException e) {
            System.err.println("Error reading DQDL rules file: " + e.getMessage());
            return "{}";
        }
    }

    private static Dataset<Row> createSampleData(SparkSession spark) {
        List<Row> data = Arrays.asList(
                org.apache.spark.sql.RowFactory.create(1, "John Doe", "john.doe@email.com", "555-0101", "Premium", 50000.0, "2024-01-15", "USA", 35, "Active"),
                org.apache.spark.sql.RowFactory.create(2, "Jane Smith", "jane.smith@email.com", "555-0102", "Standard", 25000.0, "2024-01-16", "USA", 28, "Active"),
                org.apache.spark.sql.RowFactory.create(3, "Bob Johnson", "bob.j@email.com", "555-0103", "Premium", 75000.0, "2024-01-17", "Canada", 42, "Active"),
                org.apache.spark.sql.RowFactory.create(4, "Alice Williams", null, "555-0104", "Standard", 30000.0, "2024-01-18", "USA", 31, "Active"),
                org.apache.spark.sql.RowFactory.create(1, "John Doe", "john.doe@email.com", "555-0101", "Premium", 50000.0, "2024-01-15", "USA", 35, "Active"),
                org.apache.spark.sql.RowFactory.create(11, "Invalid User", "invalid-email", "555-0111", "Unknown", -5000.0, "2024-01-25", "USA", 150, "Active"),
                org.apache.spark.sql.RowFactory.create(12, "Test User", "test@email.com", "555-0112", "Standard", 28000.0, "2024-01-26", null, 32, "Active")
        );

        return spark.createDataFrame(data, org.apache.spark.sql.types.DataTypes.createStructType(Arrays.asList(
                org.apache.spark.sql.types.DataTypes.createStructField("customer_id", org.apache.spark.sql.types.DataTypes.IntegerType, false),
                org.apache.spark.sql.types.DataTypes.createStructField("name", org.apache.spark.sql.types.DataTypes.StringType, false),
                org.apache.spark.sql.types.DataTypes.createStructField("email", org.apache.spark.sql.types.DataTypes.StringType, true),
                org.apache.spark.sql.types.DataTypes.createStructField("phone", org.apache.spark.sql.types.DataTypes.StringType, true),
                org.apache.spark.sql.types.DataTypes.createStructField("tier", org.apache.spark.sql.types.DataTypes.StringType, false),
                org.apache.spark.sql.types.DataTypes.createStructField("credit_limit", org.apache.spark.sql.types.DataTypes.DoubleType, false),
                org.apache.spark.sql.types.DataTypes.createStructField("signup_date", org.apache.spark.sql.types.DataTypes.StringType, false),
                org.apache.spark.sql.types.DataTypes.createStructField("country", org.apache.spark.sql.types.DataTypes.StringType, true),
                org.apache.spark.sql.types.DataTypes.createStructField("age", org.apache.spark.sql.types.DataTypes.IntegerType, false),
                org.apache.spark.sql.types.DataTypes.createStructField("status", org.apache.spark.sql.types.DataTypes.StringType, false)
        )));
    }
}

