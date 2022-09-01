package dev.jveloper.config;

import dev.jveloper.entities.Person;
import dev.jveloper.entities.PersonAgg;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class CustomSerdes {

    public CustomSerdes() {
    }

    public static Serde<Person> Person() {
        JsonSerializer<Person> serializer = new JsonSerializer<>();
        JsonDeserializer<Person> deserializer = new JsonDeserializer<>(Person.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<PersonAgg> PersonAgg() {
        JsonSerializer<PersonAgg> serializer = new JsonSerializer<>();
        JsonDeserializer<PersonAgg> deserializer = new JsonDeserializer<>(PersonAgg.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
