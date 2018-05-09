package com.cleo.rds;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

import javax.persistence.Temporal;
import javax.persistence.TemporalType;

public class JobStatusKey implements Serializable {
    private String tenantId;
    private String dataflowId;
    @Temporal(TemporalType.TIMESTAMP)
    private Date startDate;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobStatusKey that = (JobStatusKey) o;
        return Objects.equals(tenantId, that.tenantId) &&
                Objects.equals(dataflowId, that.dataflowId) &&
                Objects.equals(startDate, that.startDate);
    }

    @Override
    public int hashCode() {

        return Objects.hash(tenantId, dataflowId, startDate);
    }
}
