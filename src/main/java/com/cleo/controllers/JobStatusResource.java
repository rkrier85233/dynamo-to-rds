package com.cleo.controllers;

import com.cleo.dynamodb.entities.JobStatus;
import com.cleo.rds.JobStatusRds;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobStatusResource  {
    @JsonProperty("id")
    private String entityId;
    private String jobId;
    private long jobNumber;
    private String dataFlowId;
    private String dataFlowName;
    private String agentId;
    private String status;
    private Map<String, String> creatorAttributes;
    private String message;
    private long totalItems;
    private long totalBytes;
    private List<Map<String, Object>> items;
    private long totalCompleted;
    private long totalSucceeded;
    private long totalFailed;
    private long totalStopped;
    private Date startDate;
    private Date endDate;

    // Private no arg constructor needed by Jackson
    private JobStatusResource() {
    }

    public static JobStatusResource fromDynamo(JobStatus jobStatus) {
        JobStatusResource result = new JobStatusResource();
        result.entityId = jobStatus.getId();
        result.jobId = jobStatus.getJobId();
        result.jobNumber = jobStatus.getJobNumber();
        result.dataFlowId = getIndexPart(result.entityId, 1);
        result.dataFlowName = "<Not set>";
        result.agentId = jobStatus.getAccessPointId();
        result.status = jobStatus.getStatus();
        result.creatorAttributes = jobStatus.getCreatorAttributes() == null ? new HashMap<>() : jobStatus.getCreatorAttributes();
        result.creatorAttributes.put("id", jobStatus.getCreator());
        result.message = jobStatus.getMessage();
        result.totalItems = jobStatus.getTotalItems();
        result.totalBytes = jobStatus.getTotalBytes();
        result.items = jobStatus.getItems();
        result.totalCompleted = jobStatus.getTotalComplete();
        result.totalSucceeded = jobStatus.getTotalSucceeded();
        result.totalStopped = jobStatus.getTotalStopped();
        result.totalFailed = jobStatus.getTotalFailed();
        result.startDate = new Date(jobStatus.getStartDate());
        result.endDate = jobStatus.getEndDate() == null ? null : new Date(jobStatus.getEndDate());
        return result;
    }

    public static JobStatusResource fromRds(JobStatusRds jobStatus) {
        JobStatusResource result = new JobStatusResource();
        result.entityId = jobStatus.getTenantId().concat(".").concat(jobStatus.getDataflowId()).concat(".").concat(String.valueOf(jobStatus.getStartDate().getTime()));
        result.jobId = jobStatus.getJobId();
        result.jobNumber = jobStatus.getJobNumber();
        result.dataFlowId = jobStatus.getDataflowId();
        result.dataFlowName = "<Not set>";
        result.agentId = jobStatus.getAccessPointId();
        result.status = jobStatus.getStatus();
        result.creatorAttributes = jobStatus.getCreatorAttributes() == null ? new HashMap<>() : jobStatus.getCreatorAttributes();
        result.creatorAttributes.put("id", jobStatus.getCreator());
        result.message = jobStatus.getMessage();
        result.totalItems = jobStatus.getTotalItems();
        result.totalBytes = jobStatus.getTotalBytes();
        result.items = jobStatus.getItems();
        result.totalCompleted = jobStatus.getTotalComplete();
        result.totalSucceeded = jobStatus.getTotalSucceeded();
        result.totalStopped = jobStatus.getTotalStopped();
        result.totalFailed = jobStatus.getTotalFailed();
        result.startDate = jobStatus.getStartDate();
        result.endDate = jobStatus.getEndDate();
        return result;
    }

    public static String getIndexPart(String index, int idx) {
        return getIndexPart(index, idx, "#");
    }

    public static String getIndexPart(String index, int idx, String delim) {
        if (StringUtils.isNotBlank(index)) {
            String[] parts = index.split(delim);
            if (idx > parts.length) {
                return null;
            }
            return parts[idx];
        }
        return null;
    }
}
