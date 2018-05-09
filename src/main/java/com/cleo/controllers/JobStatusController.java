package com.cleo.controllers;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.cleo.dynamodb.entities.JobStatus;
import com.cleo.dynamodb.entities.JobStatusRepository;
import com.cleo.proxy.Page;
import com.cleo.proxy.PageRange;
import com.cleo.rds.JobStatusRds;
import com.cleo.rds.JobStatusRepositoryRds;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.glassfish.jersey.message.internal.MediaTypes;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/api")
public class JobStatusController {

    private static final String DYNAMODB_ENDPOINT = "https://dynamodb.us-west-2.amazonaws.com";
    private static final String REGATTA_TENANT = "us-west-2_wFxjrr446";
    private static final String CLEO_TENANT = "us-west-2_WOkzsILvZ";

    private static HashMap<String, EntityManagerFactory> emfMap = new HashMap<>();
    private static AmazonDynamoDB amazonDynamoDB;

    @GET
    @Path("{tenantId}/{dataFlowId}/jobs/dynamo")
    @Produces(MediaType.APPLICATION_JSON)
    public PagedResult<JobStatusResource> getAllJobStatusesForDataflow(@PathParam("tenantId") String tenantId,
                                                                       @PathParam("dataFlowId") String dataFlowId,
                                                                       @QueryParam("startIndex") @DefaultValue("0") int startIndex,
                                                                       @QueryParam("count") @DefaultValue("100") int count,
                                                                       @QueryParam("dateFrom") String dateFromString,
                                                                       @QueryParam("dateTo") String dateToString,
                                                                       @QueryParam("status") String status) {
        log.info("Jobs: Get all jobs request.");
        Date dateFrom = null;
        Date dateTo = null;
        if (StringUtils.isNotBlank(dateFromString)) {
            dateFrom = Date.from(Instant.parse(dateFromString));
        }
        if (StringUtils.isNotBlank(dateToString)) {
            dateTo = Date.from(Instant.parse(dateToString));
        }

        PageRange pageRange = new PageRange()
                .withStartIndex(startIndex)
                .withCount(count);

        JobStatusRepository repository = new JobStatusRepository(getAmazonDynamoDB());
        StopWatch stopWatch = StopWatch.createStarted();
        Page<JobStatus> page = repository.findAllForDataFlow(tenantId, dataFlowId, pageRange, dateFrom, dateTo, status);
        stopWatch.stop();
        log.info("DynamoDb query took: {}.", stopWatch.toString());
        List<JobStatusResource> resources = new ArrayList<>(page.getResults().size());
        page.getResults().forEach(js -> {
            resources.add(JobStatusResource.fromDynamo(js));
        });

        //vpc-b06aa3d7
        return new PagedResult<>(page.getTotalResults(), resources, startIndex, count);
    }

    @GET
    @Path("{tenantId}/{dataFlowId}/jobs/rds")
    @Produces(MediaType.APPLICATION_JSON)
    public PagedResult<JobStatusResource> getAllJobStatusesForDataflowRds(@PathParam("tenantId") String tenantId,
                                                                       @PathParam("dataFlowId") String dataFlowId,
                                                                       @QueryParam("startIndex") @DefaultValue("0") int startIndex,
                                                                       @QueryParam("count") @DefaultValue("100") int count,
                                                                       @QueryParam("dateFrom") String dateFromString,
                                                                       @QueryParam("dateTo") String dateToString,
                                                                       @QueryParam("status") String status,
                                                                          @QueryParam("clear") boolean clear) {
        // ec2: 172.31.29.23
        // lambda: 
        log.info("Jobs: Get all jobs request.");
        Date dateFrom = null;
        Date dateTo = null;
        if (StringUtils.isNotBlank(dateFromString)) {
            dateFrom = Date.from(Instant.parse(dateFromString));
        }
        if (StringUtils.isNotBlank(dateToString)) {
            dateTo = Date.from(Instant.parse(dateToString));
        }

        PageRange pageRange = new PageRange()
                .withStartIndex(startIndex)
                .withCount(count);

        String tenant = null;
        if (CLEO_TENANT.equals(tenantId)) {
            tenant = "cleo";
        } else if (REGATTA_TENANT.equals(tenantId)) {
            tenant = clear ? "regatta-clear" : "regatta";
        }

        EntityManager em = getEntityManager(tenant);
        try {
            JobStatusRepositoryRds repository = new JobStatusRepositoryRds(em);
            StopWatch stopWatch = StopWatch.createStarted();
            Page<JobStatusRds> page = repository.findAllForDataFlow(tenantId, dataFlowId, pageRange, dateFrom, dateTo, status);
            stopWatch.stop();
            log.info("RDS query took: {}.", stopWatch.toString());
            List<JobStatusResource> resources = new ArrayList<>(page.getResults().size());
            page.getResults().forEach(js -> {
                resources.add(JobStatusResource.fromRds(js));
            });

            return new PagedResult<>(page.getTotalResults(), resources, startIndex, count);
        } finally {
            em.close();
        }
    }

    private EntityManager getEntityManager(String tenant) {
        try {
            log.info("My IP: {}", InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        EntityManagerFactory emf = emfMap.computeIfAbsent(tenant, x -> {
           log.info("Creating new EntityManagerFactory for tenant: {}.", tenant);
           return Persistence.createEntityManagerFactory(tenant.toLowerCase());
        });

        log.info("Creating an entity manager using tenant: {}.", tenant);
        return emf.createEntityManager();
    }

    private AmazonDynamoDB getAmazonDynamoDB() {
        if (amazonDynamoDB == null) {
            log.info("Creating new AmazonDynamoDB.");
            AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
            builder.setCredentials(DefaultAWSCredentialsProviderChain.getInstance());
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(DYNAMODB_ENDPOINT, null));
            amazonDynamoDB = builder.build();
        }

        return amazonDynamoDB;
    }
}
