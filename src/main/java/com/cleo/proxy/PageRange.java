package com.cleo.proxy;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PageRange {
    private Integer startIndex;
    private Integer count;

    public PageRange() {
        startIndex = 0;
        count = Integer.MAX_VALUE;
    }

    public PageRange withStartIndex(int startIndex) {
        this.startIndex = startIndex;
        return this;
    }

    public PageRange withCount(int count) {
        this.count = count;
        return this;
    }
}
