package dev.jveloper;

import dev.jveloper.config.CustomSerdes;
import dev.jveloper.entities.Person;
import dev.jveloper.entities.PersonAgg;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class KafkaStreamSerdesApp {

    public static void main(String[] args) {


        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "phrase-count-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder streamsBuilder = new StreamsBuilder();


        final KStream<Integer, Person> source = streamsBuilder.stream("person", Consumed.with(Serdes.Integer(), CustomSerdes.Person()));

        final KTable<Integer, PersonAgg> ageAggregate = source
                .groupBy((key, value) -> value.getAge())
                .count()
                .mapValues(PersonAgg::new);

        ageAggregate.toStream().to("person.count", Produced.with(Serdes.Integer(), CustomSerdes.PersonAgg()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }


}
