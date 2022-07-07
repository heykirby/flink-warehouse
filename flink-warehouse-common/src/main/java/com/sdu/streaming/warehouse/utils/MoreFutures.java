package com.sdu.streaming.warehouse.utils;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

import static java.util.Collections.emptyList;

public class MoreFutures {

    private MoreFutures() {

    }

    public static List<Object> tryAwait(Collection<? extends Future<?>> futures) throws RuntimeException {
        if (futures == null || futures.isEmpty()) {
            return emptyList();
        }
        List<Object> result = new LinkedList<>();
        for (Future<?> future : futures) {
            try {
                result.add(future.get());
            } catch (Exception e) {
                throw new RuntimeException("failed get result", e);
            }
        }
        return result;
    }

}
