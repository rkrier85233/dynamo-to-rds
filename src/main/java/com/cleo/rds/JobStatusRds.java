package com.cleo.rds;

import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import lombok.Getter;
import lombok.Setter;

@Entity
@Table( name = "jobstatus")
@Getter
@Setter
@IdClass(JobStatusKey.class)
@Cacheable
public class JobStatusRds {
    @Id
    private String tenantId;
    @Id
    private String dataflowId;
    private String creator;
    @Convert(converter = JsonMapConverter.class)
    private Map<String, String> creatorAttributes;
    private String status;
    private String jobId;
    private long jobNumber;
    @Column(name="accessPointId")
    private String accessPointId;

    private String initiatedMessage;
    @Temporal(TemporalType.TIMESTAMP)
    private Date initiatedTimestamp;

    private long totalItems;
    private long totalBytes;
    @Convert(converter = JsonArrayConverter.class)
    private List<Map<String, Object>> items;
    private String detailMessage;
    @Temporal(TemporalType.TIMESTAMP)
    private Date detailTimestamp;

    private long totalComplete;
    private long totalSucceeded;
    private long totalFailed;
    private long totalStopped;
    private long totalBytesTransferred;
    private String resultMessage;
    @Temporal(TemporalType.TIMESTAMP)
    private Date resultTimestamp;

    @Id
    @Temporal(TemporalType.TIMESTAMP)
    private Date startDate;
    @Temporal(TemporalType.TIMESTAMP)
    private Date endDate;

    public JobStatusRds() {
    }

    public JobStatusRds(String tenantId, String dataflowId) {
        this.tenantId = tenantId;
        this.dataflowId = dataflowId;
    }

    public String getMessage() {
        if (resultMessage != null) {
            return resultMessage;
        } else if (detailMessage != null) {
            return detailMessage;
        }
        return initiatedMessage;
    }
}
