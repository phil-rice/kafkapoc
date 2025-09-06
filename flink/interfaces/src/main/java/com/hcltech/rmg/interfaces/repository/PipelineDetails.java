package com.hcltech.rmg.interfaces.repository;

import com.hcltech.rmg.interfaces.pipeline.ValueTC;

import java.util.LinkedHashMap;

public record PipelineDetails<From, To>(ValueTC<From> valueTC,LinkedHashMap<String, PipelineStageDetails<?, ?>> stages) {
}
