package com.hcltech.rmg.cepstate.worklease;

import com.hcltech.rmg.cepstate.partitions.PartitionContext;

public interface WorkLeaseFactory {
    WorkLease create(PartitionContext context, String ownerId);
}
