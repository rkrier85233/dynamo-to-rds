package com.cleo.rds;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.cleo.dynamodb.entities.JobStatus;
import com.cleo.proxy.Page;
import com.cleo.proxy.PageRange;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobStatusRepositoryRds {
    private EntityManager em;

    public JobStatusRepositoryRds(EntityManager em) {
        this.em = em;
    }

    public Page<JobStatusRds> findAllForDataFlow(final String organizationId,
                                              final String dataflowId,
                                              final PageRange pageRange,
                                              Date dateFrom,
                                              Date dateTo,
                                              String status) {

        if (dateTo == null) {
            dateTo = new Date();
        }
        long fromMills = dateFrom == null ? 0 : dateFrom.getTime();
        long toMills = dateTo == null ? Long.MAX_VALUE : dateTo.getTime();

        final boolean not = status != null && status.startsWith("!");
        if (not) {
            status = status.substring(1);
        }

        Map<String, Object> params = new HashMap<>();
        String statusFilter = null;
        if ("error".equalsIgnoreCase(status)) {
            params.put("status", "FAILURE");
            statusFilter = "js.status = :status and js.totalFailed = js.totalComplete";
        } else if ("partial".equalsIgnoreCase(status)) {
            params.put("status", "FAILURE");
            statusFilter = "js.status = :status and js.totalFailed < js.totalComplete";
        } else if ("inprogress".equalsIgnoreCase(status)) {
            params.put("status", "IN_PROGRESS");
            statusFilter = "js.status = :status";
        } else if ("stopped".equalsIgnoreCase(status)) {
            params.put("status", "STOPPED");
            statusFilter = "js.status = :status";
        } else if ("success".equalsIgnoreCase(status)) {
            params.put("status", "SUCCESS");
            statusFilter = "js.status = :status";
        } else if (StringUtils.isNotBlank(status)) {
            log.warn("Unknown filter status: {}, no filtering will be performed.", status);
        }

        String selectJpql = "SELECT js FROM JobStatusRds js WHERE js.tenantId = :tenantId AND js.dataflowId = :dataflowId AND js.startDate between :startDate AND :endDate";
        StringBuilder filter = new StringBuilder();
        if (statusFilter != null) {
            if (not) {
                filter.append("NOT ");
            }
            filter.append("(").append(statusFilter).append(")");
            selectJpql += " AND " + filter.toString();
        }
        selectJpql += " ORDER BY js.startDate DESC";

        TypedQuery<JobStatusRds> selectQuery = em.createQuery(selectJpql, JobStatusRds.class);
        selectQuery.setParameter("tenantId", organizationId);
        selectQuery.setParameter("dataflowId", dataflowId);
        selectQuery.setParameter("startDate", new Date(fromMills));
        selectQuery.setParameter("endDate", new Date(toMills));
        params.forEach(selectQuery::setParameter);
        selectQuery.setFirstResult(pageRange.getStartIndex());
        selectQuery.setMaxResults(pageRange.getCount());
        selectQuery.setHint("org.hibernate.cacheable", true);

        List<JobStatusRds> results = selectQuery.getResultList();

        String countJpql = "SELECT COUNT(js.tenantId) FROM JobStatusRds js WHERE js.tenantId = :tenantId AND js.dataflowId = :dataflowId AND js.startDate between :startDate AND :endDate";
        if (statusFilter != null) {
            countJpql += " AND " + filter.toString();
        }

        TypedQuery<Long> countQuery = em.createQuery(countJpql, Long.class);
        countQuery.setParameter("tenantId", organizationId);
        countQuery.setParameter("dataflowId", dataflowId);
        countQuery.setParameter("startDate", new Date(fromMills));
        countQuery.setParameter("endDate", new Date(toMills));
        params.forEach(countQuery::setParameter);
        countQuery.setHint("org.hibernate.cacheable", true);

        return new Page<>(results, countQuery.getSingleResult().intValue());
    }

    public static void main(String[] args) {

        EntityManager em = Persistence.createEntityManagerFactory("regatta-clear")
                .createEntityManager();

        JobStatusRepositoryRds repository = new JobStatusRepositoryRds(em);

        String tenantId = "us-west-2_wFxjrr446";
        String dataflowId = "0f78efae-16e8-439c-ab82-97475b300779";
        PageRange pageRange = new PageRange()
                .withStartIndex(1000)
                .withCount(500);

        StopWatch stopWatch = StopWatch.createStarted();
        Page<JobStatusRds> page = repository.findAllForDataFlow(tenantId, dataflowId, pageRange, null, new Date(), "success");
        stopWatch.stop();
        System.out.println("Took: " + stopWatch);

        em.close();
        em.getEntityManagerFactory().close();
    }
}
