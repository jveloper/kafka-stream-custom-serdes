package dev.jveloper.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import dev.jveloper.entities.Person;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

    private final Gson gson = new GsonBuilder().create();

    private Class<T> destinationClass;
    private Type reflectionTypeToken;

    public JsonDeserializer(Class<T> destinationClass) {
        this.destinationClass = destinationClass;
    }

    public JsonDeserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        try {
            if (bytes == null){
                System.out.println("Null received on deserializing object");
                return null;
            }
            System.out.println("Deserializing...");
            Type type = destinationClass != null ? destinationClass : reflectionTypeToken;
            return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), type);
        }
        catch (Exception e){
            throw new SerializationException("Error when deserializing byte[] to Person");
        }
    }

    @Override
    public void close() {
    }
}
