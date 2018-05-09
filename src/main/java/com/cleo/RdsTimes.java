package com.cleo;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.cleo.dynamodb.entities.DataFlow;
import com.cleo.dynamodb.entities.DataFlowRepository;
import com.cleo.proxy.Page;
import com.cleo.proxy.PageRange;
import com.cleo.rds.JobStatusRds;
import com.cleo.rds.JobStatusRepositoryRds;

import org.apache.commons.lang3.time.StopWatch;

import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RdsTimes {
    private static final String DYNAMODB_ENDPOINT = "https://dynamodb.us-west-2.amazonaws.com";
    private static final String[] RDS_INSTANCES = new String[] {"cleo", "regatta"};

    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            System.out.println("Must supply the page size value!");
            System.exit(1);
            return;
        }

        int pageSize = Integer.parseInt(args[0]);

        AmazonDynamoDB dynamoDB = createDynamoDB();
        DataFlowRepository dataFlowRepository = new DataFlowRepository(dynamoDB);
        for (String rdsInstance : RDS_INSTANCES) {
            log.info("RDS Instance: {}", rdsInstance);
            EntityManager em = createEntityManager(rdsInstance);
            JobStatusRepositoryRds jobStatusRepository = new JobStatusRepositoryRds(em);
            try {
                String jpql = "SELECT NEW JobStatusRds (js.tenantId, js.dataflowId) FROM JobStatusRds js GROUP BY js.tenantId, js.dataflowId";
                List<JobStatusRds> results = em.createQuery(jpql, JobStatusRds.class).getResultList();
                results.forEach(js -> {

                    DataFlow dataFlow = dataFlowRepository.findOne(js.getTenantId(), js.getDataflowId());
                    log.info("Data Flow: {}", dataFlow.getName());

                    doQuery(jobStatusRepository, dataFlow, pageSize);
                    log.info("");
                });
            }
            finally {
                close(em);
            }
        }
    }

    private static void doQuery(JobStatusRepositoryRds jobStatusRepository, DataFlow dataFlow, int pageSize) {
        log.info("limit\toffset\tcount\ttime");
        int startIndex = 0;
        long count = 0;
        while (true) {
            PageRange pageRange = new PageRange()
                    .withStartIndex(startIndex)
                    .withCount(pageSize);

            StopWatch stopWatch = StopWatch.createStarted();
            Page<JobStatusRds> page = jobStatusRepository.findAllForDataFlow(dataFlow.getOrganizationId(), dataFlow.getId(), pageRange, null, new Date(), null);
            stopWatch.stop();

            int size = page.getResults().size();
            count += size;

            log.info("{}\t{}\t{}\t{}", pageSize, startIndex, size, (stopWatch.getTime() / 1000D));
            startIndex += pageSize;

            if (count >= page.getTotalResults()) {
                return;
            }
        }
    }

    private static AmazonDynamoDB createDynamoDB() {
        AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
        builder.setCredentials(DefaultAWSCredentialsProviderChain.getInstance());
        builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(DYNAMODB_ENDPOINT, null));
        return builder.build();
    }

    private static EntityManager createEntityManager(String rdsInstance) {
        return Persistence.createEntityManagerFactory(rdsInstance.toLowerCase()).createEntityManager();
    }

    private static void close(EntityManager em) {
        em.close();
        em.getEntityManagerFactory().close();
    }
}
