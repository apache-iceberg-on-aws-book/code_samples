package com.aws.iceberg.java;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import software.amazon.awssdk.utils.ImmutableMap;

import java.io.IOException;
import java.util.*;

public class Main {
    public static void main(String[] args) {

        //Note the use of specific s3 tables configs around sigv4 signing

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        properties.put(CatalogProperties.URI, "https://s3tables.<region>.amazonaws.com/iceberg");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "arn:yourARN:bucket/namespace");
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("rest.signing-name", "s3tables");
        properties.put("rest.auth.type", "sigv4");

        RESTCatalog catalog = new RESTCatalog();

        catalog.initialize("demo", properties);

        //Create a custom namespace if it not exists

        try {
            catalog.createNamespace(Namespace.of("webapp"));
        } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
            System.out.println("Namespace already exists");
        }

        //Create a partitioned table called 'logs' into the namespace, note that this table is partitioned

        Namespace namespace = Namespace.of("webapp");
        TableIdentifier name = TableIdentifier.of(namespace, "logs");


        Schema schema = new Schema(
                Types.NestedField.required(1, "level", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "message", Types.StringType.get()),
                Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .hour("event_time")
                .build();

        try {
            catalog.createTable(name, schema, spec);
        } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
            System.out.println("Table already exists");
        }

        // list tables from the namespace

        List<TableIdentifier> tables = catalog.listTables(namespace);
        System.out.println(tables);

        // create an unpartitioned table for user events

        schema = new Schema(
                Types.NestedField.optional(1, "event_id", Types.StringType.get()),
                Types.NestedField.optional(2, "username", Types.StringType.get()),
                Types.NestedField.optional(3, "userid", Types.IntegerType.get()),
                Types.NestedField.optional(4, "api_version", Types.StringType.get()),
                Types.NestedField.optional(5, "command", Types.StringType.get())
        );

        Namespace webapp = Namespace.of("webapp");
        name = TableIdentifier.of(webapp, "user_events");

        try {
            catalog.createTable(name, schema, PartitionSpec.unpartitioned());
        } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
            System.out.println("Table already exists");
        }

        Table tbl = catalog.loadTable(name);

        //let's generate some records and insert them on a table

        GenericRecord record = GenericRecord.create(schema);
        List<GenericRecord> recordsList = new ArrayList<>();
        recordsList.add(record.copy(ImmutableMap.of("event_id", UUID.randomUUID().toString(), "username", "Bruce", "userid", 1, "api_version", "1.0", "command", "grapple")));
        recordsList.add(record.copy(ImmutableMap.of("event_id", UUID.randomUUID().toString(), "username", "Wayne", "userid", 1, "api_version", "1.0", "command", "glide")));
        recordsList.add(record.copy(ImmutableMap.of("event_id", UUID.randomUUID().toString(), "username", "Clark", "userid", 1, "api_version", "2.0", "command", "fly")));
        recordsList.add(record.copy(ImmutableMap.of("event_id", UUID.randomUUID().toString(), "username", "Kent", "userid", 1, "api_version", "1.0", "command", "land")));

        String filepath = tbl.location() + "/" + UUID.randomUUID().toString();
        OutputFile file = tbl.io().newOutputFile(filepath);
        DataWriter<GenericRecord> dataWriter =
                null;
        try {
            dataWriter = Parquet.writeData(file)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .withSpec(PartitionSpec.unpartitioned())
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            for (GenericRecord rec : recordsList) dataWriter.write(rec);
        } finally {
            try {
                dataWriter.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        DataFile dataFile = dataWriter.toDataFile();

        //when the data is written we generate a new append operation (snapshot)

        tbl.newAppend().appendFile(dataFile).commit();

        //then we can read the inserted data

        try (CloseableIterable<org.apache.iceberg.data.Record> result = IcebergGenerics.read(tbl).build()) {
            for (Record r : result) {
                System.out.println(r);
            }
        } catch (IOException e) {
            // Handle any potential IOException that might occur during closing
            throw new RuntimeException(e);
        }

    }
}
