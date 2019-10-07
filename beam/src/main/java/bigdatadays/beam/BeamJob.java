package bigdatadays.beam;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.beam.sdk.io.AvroIO;
import bigdatadays.beam.options.BeamJobOptions;
import org.apache.beam.sdk.io.clickhouse.ClickHouseIO;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class BeamJob {

    private final static String jdbcUrlTemplate = "jdbc:%s://%s:%s/%s?user=%s&password=%s";

    private static class ConstructAvroRecordsFn extends DoFn<KV<String, byte[]>, GenericRecord> {
        private final String schemaJson;
        private Schema schema;

        ConstructAvroRecordsFn(Schema schema){
            schemaJson = schema.toString();
        }

        @Setup
        public void setup(){
            schema = new Schema.Parser().parse(schemaJson);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            KV<String, byte[]> kv = (KV<String, byte[]>) c.element();
            byte[] bf = kv.getValue();
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord> (schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(bf,null);
            GenericRecord record = reader.read(null,decoder);
            c.output(record);
        }

    }

    private static class ConvertGenericRecordsToRows extends DoFn<GenericRecord, Row> {

        private final String schemaJson;

        private Schema schema;

        ConvertGenericRecordsToRows(Schema schema){
            schemaJson = schema.toString();
        }

        @Setup
        public void setup(){
            schema = new Schema.Parser().parse(schemaJson);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            GenericRecord record = (GenericRecord) c.element();
            Row row = AvroUtils.toBeamRowStrict(record,AvroUtils.toBeamSchema(schema));
            c.output(row);
        }

    }

    public static void run(BeamJobOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        Schema.Parser parser = new Schema.Parser();

        try (InputStream input = BeamJob.class.getClassLoader()
                .getResourceAsStream(options.getAvroSchemaPath())) {

            Schema schema = parser.parse(input);

            /*
            Map<String, Object> consumerProperties = ImmutableMap.of(
                    "auto.offset.reset", options.getAutoOffsetReset(),
                    "schema.registry.url", options.getSchemaRegistryUrl(),
                    "zookeeper.connect", options.getZookeeper()
            );


            PTransform<PBegin, PCollection<KV<String, GenericRecord>>> kafka = KafkaIO.<String, GenericRecord>read()
                    .withBootstrapServers(options.getKafkaBroker())
                    .withTopic(options.getKafkaTopic())
                    .withKeyDeserializer((Class) StringDeserializer.class)
                    .withValueDeserializerAndCoder(KafkaAvroDeserializer.class, AvroCoder.of(GenericRecord.class, schema))
                    .updateConsumerProperties(consumerProperties)
                    .withoutMetadata();

            PCollection<GenericRecord> inputTable = pipeline
                    .apply("Read data from Kafka", kafka)
                    .apply ("Get values",
                            Values.<GenericRecord>create());

            */


            PCollection<GenericRecord> inputTable = pipeline
                    .apply(AvroIO.readGenericRecords(schema).from(options.getInputPath()))
                    .setCoder(AvroCoder.of(GenericRecord.class, schema));

            PCollection<Row> rows =
                    inputTable
                            .apply(
                                    "Convert to PCollection<Row>",
                                    ParDo.of(new ConvertGenericRecordsToRows(schema)))
                            .setRowSchema(AvroUtils.toBeamSchema(schema));

            rows.apply(
                    "Write to ClickHouse",
                    ClickHouseIO
                            .<Row>write(String.format(jdbcUrlTemplate, options.getSubprotocol(),
                                    options.getHost(), options.getPort(), options.getDatabase(),
                                    options.getUser(), options.getPassword()), options.getTable()));


        } catch (IOException e) {
            System.out.println(String.format("File %s not found", options.getAvroSchemaPath()));
        }

        pipeline.run().waitUntilFinish();


    }

    public static void main(String[] args) {
        BeamJobOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(BeamJobOptions.class);
        run(options);
    }

}
