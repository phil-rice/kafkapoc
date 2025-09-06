package com.hcltech.rmg.interfaces.repository;

import java.util.LinkedHashMap;

public record PipelineDetails<From, To>(LinkedHashMap<String, PipelineStageDetails<?, ?>> stages) {
}
