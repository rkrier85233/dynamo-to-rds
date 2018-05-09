package com.cleo.dynamodb.entities;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.ArrayList;
import java.util.List;

public class DynamoListConverter implements DynamoDBTypeConverter<List<AttributeValue>, List<Object>> {
    @Override
    public List<AttributeValue> convert(List<Object> list) {
        if (list == null) {
            return null;
        }

        DynamoMapConverter mapConverter = new DynamoMapConverter();
        List<AttributeValue> result = new ArrayList<>(list.size());
        list.forEach(i -> {
            AttributeValue attributeValue = mapConverter.convertValue(i);
            if (attributeValue != null) {
                result.add(attributeValue);
            }
        });

        return result;
    }

    @Override
    public List<Object> unconvert(List<AttributeValue> attributeValues) {
        DynamoMapConverter mapConverter = new DynamoMapConverter();
        List<Object> results = new ArrayList<>(attributeValues.size());
        attributeValues.forEach(av -> results.add(mapConverter.unconvertValue(av)));
        return results;
    }
}
