package bigdatadays.beam.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface BeamJobOptions extends PipelineOptions {

    @Description("The Kafka topic to read from")
    String getKafkaTopic();

    void setKafkaTopic(String value);

    @Description("The Kafka Broker to read from")
    String getKafkaBroker();

    void setKafkaBroker(String value);

    @Description("The Zookeeper server to connect to")
    @Default.String("")
    String getZookeeper();

    void setZookeeper(String value);

    @Description("auto.offset.reset")
    @Default.String("earliest")
    String getAutoOffsetReset();

    void setAutoOffsetReset(String value);

    @Description("schema.registry.url")
    @Default.String("http://localhost:8081")
    String getSchemaRegistryUrl();

    void setSchemaRegistryUrl(String value);

    @Description("The path to AVRO schema file")
    String getAvroSchemaPath();

    void setAvroSchemaPath(String value);

    @Description("The path to read input files")
    @Default.String("")
    String getInputPath();

    void setInputPath(String value);

    @Description("ClickHouse subprotocol")
    @Default.String("clickhouse")
    String getSubprotocol();

    void setSubprotocol(String value);

    @Description("ClickHouse host")
    String getHost();

    void setHost(String value);

    @Description("ClickHouse port")
    @Default.String("8123")
    String getPort();

    void setPort(String value);

    @Description("Username to connect to ClickHouse database")
    @Default.String("default")
    String getUser();

    void setUser(String value);

    @Description("Password")
    @Default.String("")
    String getPassword();

    void setPassword(String value);

    @Description("ClickHouse database")
    @Default.String("default")
    String getDatabase();

    void setDatabase(String value);

    @Description("ClickHouse table")
    String getTable();

    void setTable(String value);

}
