package com.cleo.dynamodb.entities;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.cleo.proxy.Page;
import com.cleo.proxy.PageRange;

import java.io.Serializable;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class DynamoDBRepository<T, ID extends Serializable> {
    private static final Long DEFAULT_READ_CAPACITY_UNITS = 1L;
    private static final Long DEFAULT_WRITE_CAPACITY_UNITS = 1L;
    private static final ProvisionedThroughput DEFAULT_PROVISIONED_THROUGHPUT = new ProvisionedThroughput().withReadCapacityUnits(DEFAULT_READ_CAPACITY_UNITS)
            .withWriteCapacityUnits(DEFAULT_WRITE_CAPACITY_UNITS);


    @Getter(AccessLevel.PUBLIC)
    private final AmazonDynamoDB dynamoDB;
    @Getter(AccessLevel.PUBLIC)
    private final DynamoDBMapper mapper;
    private final Class<T> type;
    private final DynamoDBMapperConfig config;

    protected DynamoDBRepository(AmazonDynamoDB dynamoDB, Class<T> type) {
        this.dynamoDB = dynamoDB;
        this.type = type;
        this.config = createConfig();
        this.mapper = new DynamoDBMapper(dynamoDB, config);
    }

    public T save(T entity) {
        mapper.save(entity);
        return entity;
    }

    public List<T> save(List<T> entities) {
        entities.forEach(mapper::save);
        return entities;
    }

    public T findOne(ID id) {
        return mapper.load(type, id);
    }

    public List<T> findAll() {
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
        return mapper.scan(type, scanExpression);
    }

    public void delete(T entity) {
        mapper.delete(entity);
    }

    public boolean exists(ID id) {
        return findOne(id) != null;
    }

    public void delete(ID id) {
        mapper.delete(findOne(id));
    }

    public void deleteAll() {
        delete(findAll());
    }

    public void delete(Iterable<T> entities) {
        entities.forEach(mapper::delete);
    }

    public long count() {
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
        return mapper.scan(type, scanExpression).size();
    }

    public String getTableName() {
        return getTableName(type);
    }

    public String getTableName(Class type) {
        if (config.getTableNameResolver() == null) {
            return DynamoDBMapperConfig.DefaultTableNameResolver.INSTANCE.getTableName(type, config);
        }
        return config.getTableNameResolver().getTableName(type, config);
    }

    public static DynamoDBMapperConfig createConfig() {
        String prefix = System.getProperty("amazon.dynamodb.table.prefix");
        if (prefix == null) {
            return DynamoDBMapperConfig.DEFAULT;
        }
        prefix = prefix.endsWith(".") ? prefix : prefix.concat(".");
        return
                new DynamoDBMapperConfig.Builder().withTableNameOverride(DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix(prefix))
                        .build();
    }

    public void createTable() {
        final DynamoDB dynamoDB = new DynamoDB(this.dynamoDB);
        final DynamoDBMapperConfig mapperConfig = createConfig();
        final DynamoDBMapper mapper = new DynamoDBMapper(this.dynamoDB, mapperConfig);
        final CreateTableRequest request = mapper.generateCreateTableRequest(type).withProvisionedThroughput(DEFAULT_PROVISIONED_THROUGHPUT);
        final String tableName = request.getTableName();
        try {
            final Table table = dynamoDB.createTable(request);
            table.waitForActive();
            log.info("Create table: {} for class: {}, succeeded", tableName, type.getName());
        } catch (ResourceInUseException e) {
            log.info("Table: {} for class: {}, already exists.", tableName, type.getName());
        } catch (Exception e) {
            log.error("Unable to create table {}, cause: {}", tableName, e, e);
        }
    }

    public PaginatedQueryList<T> findWithQuery(DynamoDBQueryExpression<T> queryExpression) {
        return mapper.query(type, queryExpression);
    }

    public Page<T> findWithQuery(DynamoDBQueryExpression<T> queryExpression, PageRange pageRange) {
        preQuery(queryExpression);
        queryExpression.setLimit(pageRange.getCount());

        // You must access the results with an iterator in order to discard previous pages.
        DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
                .withPaginationLoadingStrategy(DynamoDBMapperConfig.PaginationLoadingStrategy.ITERATION_ONLY)
                .build();

        PaginatedQueryList<T> pageList = mapper.query(type, queryExpression, config);
        // You must access the results with an iterator in order to discard previous pages.
        List<T> results = StreamSupport.stream(Spliterators.spliteratorUnknownSize(pageList.iterator(), Spliterator.ORDERED),
                false).skip(pageRange.getStartIndex()).limit(pageRange.getCount()).collect(Collectors.toList());

        Integer totalResults = mapper.count(type, queryExpression);
        return new Page<>(results, totalResults);
    }

    protected void preQuery(DynamoDBQueryExpression<T> queryExpression) {
        // Do nothing;
    }
}
