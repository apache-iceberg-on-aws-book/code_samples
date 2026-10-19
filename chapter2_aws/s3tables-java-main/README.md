# s3tables-java
Code showing how you can use S3Tables using Java Iceberg libraries along S3Tables IRC endpoint. 

Java API can be found [here](https://iceberg.apache.org/docs/latest/api/).

First we need to configure the catalog and initialize it, note that we need to sign the calls to the catalog using sigv4 and that we need to use s3tables as signing name:


```java
Map<String, String> properties = new HashMap<>();
properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
properties.put(CatalogProperties.URI, "https://s3tables.<region>.amazonaws.com/iceberg");
properties.put(CatalogProperties.WAREHOUSE_LOCATION, "arn:yourARN:bucket/namespace");
properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
properties.put("rest.auth.type", "sigv4");
properties.put("rest.signing-name", "s3tables");

RESTCatalog catalog = new RESTCatalog();
catalog.initialize("demo", properties);
```
The
n we can create a namespace webapp on the catalog:

```java
//Create a custom namespace if it not exists
try {
    catalog.createNamespace(Namespace.of("webapp"));
} catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
    System.out.println("Namespace already exists");
}
```
Next, we are going to create a partitioned table into the namespace called ```logs```. Note how you can Created a partitioned table using a PartitionSpec. You can also see how the schema is declared in this code block:


```java
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
```

Next, we are going to create a unpartitioned table into the namespace called ```user_events```:

```java
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
```

Then we can list the tables that are present on the namespace with this code:

```java
// list tables from the namespace
List<TableIdentifier> tables = catalog.listTables(namespace);
System.out.println(tables);
```

Let’s insert the data, note that we are using the Table operation append, this will generate a new snapshot on the table:

```java
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
            .createWriterFunc(GenericParquetWriter::buildWriter)
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
```

Then we can read the table contents using this simple code:

```java
//then we can read the inserted data
try (CloseableIterable<org.apache.iceberg.data.Record> result = IcebergGenerics.read(tbl).build()) {
    for (Record r : result) {
        System.out.println(r);
    }
} catch (IOException e) {
    // Handle any potential IOException that might occur during closing
    throw new RuntimeException(e);
}
```

When you run the code you will see on the console the inserted records:

```bash
Record(afe1f04c-bb6e-4ce6-9ff7-282c67532582, Bruce, 1, 1.0, grapple)
Record(9731629b-0d96-41c6-80a7-76c99a6dbccf, Wayne, 1, 1.0, glide)
Record(9f201985-60ef-467e-8117-fb4a155d3b99, Clark, 1, 2.0, fly)
Record(d05b8ab5-36ea-42c3-b73f-a7e5d2b6d01b, Kent, 1, 1.0, land)
Record(761815a3-e970-4556-94f2-b38526d92e81, Bruce, 1, 1.0, grapple)
Record(eb7edf7c-07d2-415e-9661-35dc02fa305d, Wayne, 1, 1.0, glide)
```
