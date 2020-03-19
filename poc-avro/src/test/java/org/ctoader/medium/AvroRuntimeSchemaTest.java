package org.ctoader.medium;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class AvroRuntimeSchemaTest {

    @Test
    public void testWriteGenericRecords() throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src/test/resources/schema/user.avsc"));

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");

        // Serialize user1 and user2 to disk
        File file = File.createTempFile("avro-user", null);
        ;
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, file);
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
            dataFileWriter.flush();
        }

        Assert.assertTrue(file.exists());
        Assert.assertTrue(file.length() > 0);
    }

    @Test
    public void testReadGenericRecords() throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src/test/resources/schema/user.avsc"));
        File exportFile = new File("src/test/resources/samples/avro-user.export");

        int entriesCount = 0;

        // Deserialize users from disk
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(exportFile, datumReader)) {
            GenericRecord user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);

                Assert.assertNotNull(user);
                Assert.assertNotNull(user.get("name"));
                Assert.assertNotNull(user.get("favorite_number"));

                entriesCount++;
            }
        }

        Assert.assertEquals(2, entriesCount);
    }
}
