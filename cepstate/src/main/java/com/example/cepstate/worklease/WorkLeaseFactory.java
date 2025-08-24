package com.example.cepstate.worklease;

import com.example.cepstate.partitions.PartitionContext;

public interface WorkLeaseFactory {
    WorkLease create(PartitionContext context, String ownerId);
}
