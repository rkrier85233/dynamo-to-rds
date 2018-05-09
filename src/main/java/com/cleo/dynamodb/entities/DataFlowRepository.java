package com.cleo.dynamodb.entities;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;


public class DataFlowRepository extends DynamoDBRepository<DataFlow, String> {
    public DataFlowRepository(AmazonDynamoDB dynamoDB) {
        super(dynamoDB, DataFlow.class);
    }

    public DataFlow findOne(String tenantId, String dataflowId) {
        return getMapper().load(DataFlow.class, tenantId, dataflowId);
    }
}

