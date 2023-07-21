package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.RelOptTable;
import org.apache.flink.calcite.shaded.org.checkerframework.checker.nullness.qual.Nullable;

public class RelEmptyColumnOrigin extends RelColumnOrigin {

    public static final RelEmptyColumnOrigin EMPTY = new RelEmptyColumnOrigin(null, -1, false);

    private RelEmptyColumnOrigin(RelOptTable originTable, int iOriginColumn, boolean isDerived) {
        super(originTable, iOriginColumn, isDerived);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (!(obj instanceof RelColumnOrigin)) {
            return false;
        }
        return obj == EMPTY;
    }

    @Override
    public int hashCode() {
        return RelEmptyColumnOrigin.class.getName().hashCode();
    }
}
