package com.cleo.dynamodb.entities;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonArrayConverter implements DynamoDBTypeConverter<List<String>, List<Map<String, Object>>> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public List<String> convert(List<Map<String, Object>> object) {
        return object.stream().map(m -> {
            try {
                return OBJECT_MAPPER.writeValueAsString(m);
            } catch (JsonProcessingException e) {
                log.warn("Unable to convert map to JSON string, cause: {}", e, e);
                return null;
            }
        }).collect(Collectors.toList());
    }

    @Override
    public List<Map<String, Object>> unconvert(List<String> object) {
        final TypeReference<Map<String, Object>> typeRef
            = new TypeReference<Map<String, Object>>() {
        };

        return object.stream().map(s -> {
            try {
                return OBJECT_MAPPER.<Map<String, Object>>readValue(s, typeRef);
            } catch (IOException e) {
                log.warn("Unable to convert JSON string to Map<String, Object>, cause: {}", e, e);
                return null;
            }
        }).collect(Collectors.toList());
    }
}