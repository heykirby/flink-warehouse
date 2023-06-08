package com.sdu.warehouse.api.lineage;

public interface LineageReporter {

    boolean reportLineage(Lineage lineage);

    boolean dropLineage(String jobName);

}
