package com.cleo.proxy;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

@Getter
public class Page<T> {
    @Setter
    private List<T> results;
    private Integer totalResults;

    public Page(List<T> results, Integer totalResults) {
        this.results = results;
        this.totalResults = totalResults;
    }
}
