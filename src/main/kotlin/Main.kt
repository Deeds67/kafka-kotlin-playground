import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration
import java.time.LocalDateTime
import java.util.*
import kotlin.concurrent.fixedRateTimer


fun main(args: Array<String>) {
    fun createProducer(): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["acks"] = "all"
        props["retries"] = 0
        props["linger.ms"] = 1
        props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        return KafkaProducer(props)
    }

    fun Producer<String, String>.produceMessages(topic: String) {
        fixedRateTimer(daemon = true, period = Duration.ofSeconds(2).toMillis()) {
            val time = LocalDateTime.now()
            val message = ProducerRecord(
                topic, // topic
                time.toString(), // key
                "Message sent on $topic at ${LocalDateTime.now()}" // value
            )
            send(message)
        }
    }

    fun createConsumer(): Consumer<String, String> {
        val props = Properties()
        props.setProperty("bootstrap.servers", "localhost:9092")
        props.setProperty("group.id", "test")
        props.setProperty("enable.auto.commit", "true")
        props.setProperty("auto.commit.interval.ms", "1000")
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        return KafkaConsumer(props)
    }

    fun Consumer<String, String>.consumeMessages(topic: String) {
        subscribe(listOf(topic))
        while (true) {
            val messages: ConsumerRecords<String, String> = poll(Duration.ofMillis(5000))
            for (message in messages) {
                println("Consumer reading message: $message")
            }
            commitAsync()
        }
    }

    val producer1 = createProducer()
    producer1.produceMessages("topic1")
    val producer2 = createProducer()
    producer2.produceMessages("topic2")

    print("producers started"
    )

    // setup a simple Flink pipeline reading data from Kafka topic1 and topic2
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()

    val source1: KafkaSource<String> = KafkaSource.builder<String>()
        .setBootstrapServers("localhost:9092")
        .setTopics("topic1")
        .setGroupId("test")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(SimpleStringSchema())
        .build()

    val source2: KafkaSource<String> = KafkaSource.builder<String>()
        .setBootstrapServers("localhost:9092")
        .setTopics("topic2")
        .setGroupId("test")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(SimpleStringSchema())
        .build()



    val topic1Stream: DataStream<String> = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Topic1 Source")
    val topic2Stream: DataStream<String> = env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Topic2 Source")
    topic1Stream.print()
    topic2Stream.print()

    env.execute();

}