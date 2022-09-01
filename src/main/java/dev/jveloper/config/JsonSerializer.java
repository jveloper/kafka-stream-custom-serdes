package dev.jveloper.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {

    private final Gson gson = new GsonBuilder().create();

    public JsonSerializer() {
    }

    @Override
    public void close() {

    }

    @Override
    public byte[] serialize(String s, T data) {

        try {
            if (data == null){
                System.out.println("Receive a null person object");
                return null;
            }
            System.out.println("Serializing");
            return gson.toJson(data).getBytes(StandardCharsets.UTF_8);

        } catch (Exception e) {
            System.out.println("Error on Serializing Person Object");
            throw new SerializationException("Error serializing JSON message", e);        }

    }

    @Override
    public void configure(Map configs, boolean isKey) {

    }
}
