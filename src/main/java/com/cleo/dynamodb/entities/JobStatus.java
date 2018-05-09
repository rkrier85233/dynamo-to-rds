package com.cleo.dynamodb.entities;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDocument;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverted;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@DynamoDBTable(tableName = "crowsnest-dataflow-dev.JobStatus")
@DynamoDBDocument
public class JobStatus {
    @DynamoDBHashKey
    private String id;
    private String creator;
    private Map<String, String> creatorAttributes;
    private String status;
    private String jobId;
    private long jobNumber;
    private String accessPointId;

    private String initiatedMessage;
    private Long initiatedTimestamp;

    private long totalItems;
    private long totalBytes;
    @DynamoDBTypeConverted(converter = JsonArrayConverter.class)
    private List<Map<String, Object>> items;
    private String detailMessage;
    private Long detailTimestamp;

    private long totalComplete;
    private long totalSucceeded;
    private long totalFailed;
    private long totalStopped;
    private long totalBytesTransferred;
    private String resultMessage;
    private Long resultTimestamp;

    @DynamoDBRangeKey
    private Long startDate;
    private Long endDate;

    @DynamoDBIgnore
    public String getMessage() {
        if (resultMessage != null) {
            return resultMessage;
        } else if (detailMessage != null) {
            return detailMessage;
        }
        return initiatedMessage;
    }
}