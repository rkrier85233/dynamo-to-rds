package com.cleo.controllers;


import javax.ws.rs.container.ContainerRequestFilter;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jrestless.aws.gateway.GatewayFeature;
import com.jrestless.aws.gateway.handler.GatewayRequestAndLambdaContext;
import com.jrestless.aws.gateway.handler.GatewayRequestObjectHandler;
import com.jrestless.aws.gateway.io.GatewayResponse;
import com.jrestless.core.container.io.JRestlessContainerRequest;

/**
 * The request handler as lambda function.
 *
 * @author Bjoern Bilger
 *
 */
public class RequestHandler extends GatewayRequestObjectHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

    public RequestHandler() {
        // configure the application with the resource
        ResourceConfig config = new ResourceConfig()
                .register(GatewayFeature.class)
                .register(JacksonFeature.class)
                .packages("com.cleo.controllers")
                .register((ContainerRequestFilter) request -> {
                    LOG.info("baseUri: " + request.getUriInfo().getBaseUri());
                    LOG.info("requestUri: " + request.getUriInfo().getRequestUri());
                });
        init(config);
        start();
    }

    @Override
    protected void beforeHandleRequest(GatewayRequestAndLambdaContext request,
                                       JRestlessContainerRequest containerRequest) {
        LOG.info("start to handle request: " + request.getGatewayRequest());
    }

    @Override
    protected GatewayResponse onRequestSuccess(GatewayResponse response, GatewayRequestAndLambdaContext request,
                                               JRestlessContainerRequest containerRequest) {
        LOG.info("request handled successfully: " + response);
        return response;
    }
}
