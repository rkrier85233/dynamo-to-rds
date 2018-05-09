package com.cleo;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.cleo.dynamodb.entities.DataFlow;
import com.cleo.dynamodb.entities.DataFlowRepository;
import com.cleo.dynamodb.entities.JobStatus;
import com.cleo.dynamodb.entities.JobStatusRepository;
import com.cleo.rds.JobStatusRds;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.persistence.EntityManager;
import javax.persistence.Persistence;

import static com.cleo.controllers.JobStatusResource.getIndexPart;

public class CopyApp {
    private static final String DYNAMODB_ENDPOINT = "https://dynamodb.us-west-2.amazonaws.com";
    private static final String REGATTA_TENANT = "us-west-2_wFxjrr446";
    private static final String CLEO_TENANT = "us-west-2_WOkzsILvZ";

    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            System.out.println("Enter a tenant (Cleo or Regatta)");
            System.exit(1);
            return;
        }

        String tenant = args[0];
        if (!"cleo".equalsIgnoreCase(tenant) && !"regatta".equalsIgnoreCase(tenant) && !"regatta-clear".equalsIgnoreCase(tenant)) {
            System.out.println(tenant + " is invalid.  Enter 'cleo' or 'regatta'.");
            System.exit(1);
            return;
        }

        String tenantId = "cleo".equalsIgnoreCase(tenant) ? CLEO_TENANT : REGATTA_TENANT;
        System.out.println("Copying job records for '" + tenant + "'.");

        AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
        builder.setCredentials(DefaultAWSCredentialsProviderChain.getInstance());
        builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(DYNAMODB_ENDPOINT, null));
        AmazonDynamoDB amazonDynamoDB = builder.build();

        EntityManager em = Persistence.createEntityManagerFactory(tenant.toLowerCase())
                .createEntityManager();

        List<DataFlow> results = findLongevityDfs(amazonDynamoDB, tenantId);
        results.forEach(df -> {
            System.out.println("Copying job records for data flow: " + df.getName());
            copyJobStatus(amazonDynamoDB, em, df);
        });

        em.close();
        em.getEntityManagerFactory().close();

        System.out.println("Done");
    }

    private static List<DataFlow> findLongevityDfs(AmazonDynamoDB amazonDynamoDB, String tenantId) {
        DataFlowRepository repository = new DataFlowRepository(amazonDynamoDB);

        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":organizationId", new AttributeValue().withS(tenantId));
        eav.put(":jobCount", new AttributeValue().withN("2000"));

        String keyConditionExpression = "organizationId = :organizationId";
        DynamoDBQueryExpression<DataFlow> queryExpression = new DynamoDBQueryExpression<DataFlow>()
                .withKeyConditionExpression(keyConditionExpression)
                .withFilterExpression("jobCount > :jobCount")
                .withExpressionAttributeValues(eav)
                .withConsistentRead(false);

        return repository.findWithQuery(queryExpression);
    }

    private static void copyJobStatus(AmazonDynamoDB amazonDynamoDB, EntityManager em, DataFlow df) {
        JobStatusRepository repository = new JobStatusRepository(amazonDynamoDB);

        String id = df.getOrganizationId().concat("#").concat(df.getId());
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":id", new AttributeValue().withS(id));

        String keyConditionExpression = "id = :id";
        DynamoDBQueryExpression<JobStatus> queryExpression = new DynamoDBQueryExpression<JobStatus>()
                .withKeyConditionExpression(keyConditionExpression)
                .withExpressionAttributeValues(eav)
                .withConsistentRead(false);

        List<JobStatus> results = repository.findWithQuery(queryExpression);

        final int size = results.size();

        em.getTransaction().begin();
        AtomicInteger count = new AtomicInteger();
        results.forEach(djs -> {
            JobStatusRds rjs = new JobStatusRds();
            String tenantId = getIndexPart(djs.getId(), 0);
            String dataflowId = getIndexPart(djs.getId(), 1);
            rjs.setTenantId(tenantId);
            rjs.setDataflowId(dataflowId);
            rjs.setCreator(djs.getCreator());
            rjs.setCreatorAttributes(djs.getCreatorAttributes());
            rjs.setStatus(djs.getStatus());
            rjs.setJobId(djs.getJobId());
            rjs.setJobNumber(djs.getJobNumber());
            rjs.setAccessPointId(djs.getAccessPointId());
            rjs.setInitiatedMessage(djs.getInitiatedMessage());
            rjs.setInitiatedTimestamp(toDate(djs.getInitiatedTimestamp()));
            rjs.setTotalItems(djs.getTotalItems());
            rjs.setTotalBytes(djs.getTotalBytes());
            rjs.setItems(djs.getItems());
            rjs.setDetailMessage(djs.getDetailMessage());
            rjs.setDetailTimestamp(toDate(djs.getDetailTimestamp()));
            rjs.setTotalComplete(djs.getTotalComplete());
            rjs.setTotalSucceeded(djs.getTotalSucceeded());
            rjs.setTotalFailed(djs.getTotalFailed());
            rjs.setTotalStopped(djs.getTotalStopped());
            rjs.setTotalBytesTransferred(djs.getTotalBytesTransferred());
            rjs.setResultMessage(djs.getResultMessage());
            rjs.setResultTimestamp(toDate(djs.getResultTimestamp()));
            rjs.setStartDate(toDate(djs.getStartDate()));
            rjs.setEndDate(toDate(djs.getEndDate()));
            em.persist(rjs);
            if (count.incrementAndGet() % 100 == 0) {
                em.getTransaction().commit();
                em.getTransaction().begin();
                System.out.println("Inserted " + count.get() + " of " + size + " job records.");
            }
        });

        if (em.getTransaction().isActive()) {
            em.getTransaction().commit();
            System.out.println("Inserted " + count.get() + " of " + size + " job records.");
        }
    }

    private static Date toDate(Long value) {
        return value == null ? null : new Date(value);
    }
}
