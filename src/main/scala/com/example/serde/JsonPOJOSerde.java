package com.example.serde;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonPOJOSerde<T> implements Serde<T>, Serializer<T>, Deserializer<T>, Configurable {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Class<T> clazz;

    private JsonPOJOSerde(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        configure(configs);
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data != null) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new SerializationException("Data could not be serialized for topic " + topic, e);
            }
        } else {
            return null;
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        if (headers.toArray().length > 0)
            throw new IllegalArgumentException("Headers not supported");
        else
            return serialize(topic, data);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data != null) {
            try {
                return objectMapper.readValue(data, clazz);
            } catch (IOException e) {
                throw new SerializationException("Data could not be deserialized for topic " + topic, e);
            }
        } else {
            return null;
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        if (headers.toArray().length > 0)
            throw new IllegalArgumentException("Headers not supported");
        else
            return deserialize(topic, data);
    }

    @Override
    public void close() {
    }

    public static <T> JsonPOJOSerde<T> serde(Class<T> clazz, boolean isKey) {
        JsonPOJOSerde<T> serde = new JsonPOJOSerde<T>(clazz);
        serde.configure(Collections.emptyMap(), isKey);
        return serde;
    }
}
