package com.cleo.controllers;

import java.util.List;

import lombok.Getter;

@Getter
public class PagedResult<T> {
    private Integer totalResults;
    private List<T> resources;
    private int count;
    private int startIndex;

    private PagedResult() {
    }

    public PagedResult(Integer totalResults, List<T> resources, int startIndex, int count) {
        this.totalResults = totalResults;
        this.resources = resources;
        this.count = count;
        this.startIndex = startIndex;
    }
}
