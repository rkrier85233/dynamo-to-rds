package com.cleo.dynamodb.entities;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamoMapConverter implements DynamoDBTypeConverter<Map<String, AttributeValue>, Map<String, Object>> {
    @Override
    public Map<String, AttributeValue> convert(Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        Map<String, AttributeValue> result = new HashMap<>(map.size());
        map.forEach((k, v) -> {
            result.put(k, convertValue(v));
        });

        return result;
    }

    @Override
    public Map<String, Object> unconvert(Map<String, AttributeValue> attributeValueMap) {
        if (attributeValueMap == null) {
            return null;
        }

        Map<String, Object> result = new HashMap<>(attributeValueMap.size());
        attributeValueMap.forEach((k, v) -> {
            AttributeValue attributeValue = convertValue(v);
            if (attributeValue != null) {
                result.put(k, attributeValue);
            }
        });

        return result;
    }

    public AttributeValue convertValue(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof String) {
            return new AttributeValue().withS((String) object);
        }
        if (object instanceof Number) {
            return new AttributeValue().withN(object.toString());
        }
        if (object instanceof ByteBuffer) {
            return new AttributeValue().withB((ByteBuffer) object);
        }
        if (object instanceof Boolean) {
            return new AttributeValue().withBOOL((Boolean) object);
        }
        if (object instanceof List) {
            List<?> list = (List<?>) object;
            if (list.isEmpty()) {
                return null;
            }
            Object first = list.get(0);
            AttributeValue attributeValue = new AttributeValue();
            if (first instanceof String) {
                return attributeValue.withSS(list.stream()
                        .map(String::valueOf)
                        .collect(Collectors.toList()));
            }
            if (first instanceof Number) {
                return attributeValue.withNS(list.stream()
                        .map(String::valueOf)
                        .collect(Collectors.toList()));
            }
            if (first instanceof ByteBuffer) {
                return attributeValue.withBS(list.stream()
                        .map(o -> (ByteBuffer) o)
                        .collect(Collectors.toList()));
            }
        }
        return null;
    }

    public Object unconvertValue(AttributeValue attributeValue) {
        if (attributeValue.getS() != null)
            return attributeValue.getS();
        if (attributeValue.getN() != null)
            return toNumber(attributeValue.getN());
        if (attributeValue.getB() != null)
            return attributeValue.getB();
        if (attributeValue.getSS() != null)
            return attributeValue.getSS();
        if (attributeValue.getNS() != null) {
            List<String> attributeValues = attributeValue.getNS();
            List<Object> results = new ArrayList<>(attributeValues.size());
            attributeValues.forEach(s -> {
                results.add(toNumber(s));
            });
            return results;
        }
        if (attributeValue.getBS() != null)
            return attributeValue.getBS();
        if (attributeValue.getM() != null)
            return unconvert(attributeValue.getM());
        if (attributeValue.getL() != null) {
            List<AttributeValue> attributeValues = attributeValue.getL();
            List<Object> results = new ArrayList<>(attributeValues.size());
            attributeValues.forEach(av -> {
                results.add(unconvertValue(av));
            });
            return results;
        }
        if (attributeValue.getNULL() != null)
            return null;
        if (attributeValue.getBOOL() != null)
            return attributeValue.getBOOL();
        return null;
    }

    private static Number toNumber(String attributeValue) {
        if (attributeValue.contains(".")) {
            Double dValue = Double.parseDouble(attributeValue);
            if (dValue.floatValue() < dValue) {
                return dValue;
            } else {
                return dValue.floatValue();
            }
        }

        Long lValue = Long.parseLong(attributeValue);
        if (lValue.intValue() < lValue) {
            return lValue;
        } else {
            return lValue.intValue();
        }
    }
}