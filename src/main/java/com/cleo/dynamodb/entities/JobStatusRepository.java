package com.cleo.dynamodb.entities;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.cleo.proxy.Page;
import com.cleo.proxy.PageRange;

import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobStatusRepository extends DynamoDBRepository<JobStatus, String> {
    public JobStatusRepository(AmazonDynamoDB dynamoDB) {
        super(dynamoDB, JobStatus.class);
    }

    /**
     * Returns all jobs for a given data flow sorted by start date descending.
     *
     * @param organizationId the organization owner tenant of the job history.
     * @param dataflowId     all jobs associated with the data flow's ID.
     * @param pageRange      page starting position and countAll.
     * @param dateTo         @return all jobs for a given data flow sorted by start date descending.
     */
    public Page<JobStatus> findAllForDataFlow(final String organizationId,
                                              final String dataflowId,
                                              final PageRange pageRange,
                                              Date dateFrom,
                                              Date dateTo,
                                              String status) {
        final String hashKey = organizationId.concat("#").concat(dataflowId);
        long fromMills = dateFrom == null ? 0 : dateFrom.getTime();
        long toMills = dateTo == null ? Long.MAX_VALUE : dateTo.getTime();

        final Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":id", new AttributeValue().withS(hashKey));
        eav.put(":fromMills", new AttributeValue().withN(String.valueOf(fromMills)));
        eav.put(":toMills", new AttributeValue().withN(String.valueOf(toMills)));

        final String expression = "id = :id and startDate between :fromMills and :toMills";

        final boolean not = status != null && status.startsWith("!");
        if (not) {
            status = status.substring(1);
        }

        final NameMap nameMap = new NameMap();
        String statusFilter = null;
        if ("error".equalsIgnoreCase(status)) {
            nameMap.put("#status", "status");
            eav.put(":status", new AttributeValue().withS("FAILURE"));
            statusFilter = "#status = :status and totalFailed = totalComplete";
        } else if ("partial".equalsIgnoreCase(status)) {
            nameMap.put("#status", "status");
            eav.put(":status", new AttributeValue().withS("FAILURE"));
            statusFilter = "#status = :status and totalFailed < totalComplete";
        } else if ("inprogress".equalsIgnoreCase(status)) {
            nameMap.put("#status", "status");
            eav.put(":status", new AttributeValue().withS("IN_PROGRESS"));
            statusFilter = "#status = :status";
        } else if ("stopped".equalsIgnoreCase(status)) {
            nameMap.put("#status", "status");
            eav.put(":status", new AttributeValue().withS("STOPPED"));
            statusFilter = "#status = :status";
        } else if ("success".equalsIgnoreCase(status)) {
            nameMap.put("#status", "status");
            eav.put(":status", new AttributeValue().withS("SUCCESS"));
            statusFilter = "#status = :status";
        } else if (StringUtils.isNotBlank(status)) {
            log.warn("Unknown filter status: {}, no filtering will be performed.", status);
        }

        StringBuilder filter = new StringBuilder();
        if (statusFilter != null) {
            if (not) {
                filter.append("NOT ");
            }
            filter.append("(").append(statusFilter).append(")");
        }

        final DynamoDBQueryExpression<JobStatus> queryExpression = new DynamoDBQueryExpression<JobStatus>()
                .withKeyConditionExpression(expression)
                .withExpressionAttributeValues(eav)
                .withFilterExpression(StringUtils.trimToNull(filter.toString()))
                .withExpressionAttributeNames(nameMap.isEmpty() ? null : nameMap)
                .withScanIndexForward(false);

        return findWithQuery(queryExpression, pageRange);
    }
}

