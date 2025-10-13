package com.hcltech.rmg.config.enrichment;

import com.hcltech.rmg.dag.ProducerValidation;
import org.junit.jupiter.api.Test;

import static com.hcltech.rmg.config.enrichment.dag.FixedEnrichmentFixture.*;
import static com.hcltech.rmg.dag.ProducerValidation.validate;
import static org.junit.jupiter.api.Assertions.*;

public class FixedEnrichmentValidationIT {

  @Test
  void distinctOutputs_areValid() {
    assertDoesNotThrow(() -> validate(set(A,B,C,D,E,X,Y), ptc(), ntc()));
  }

    @Test
    void duplicateOutput_isError() {
        // B and B_DUP both produce ["b"]
        assertThrows(IllegalArgumentException.class,
                () -> ProducerValidation.validate(set(B, B_DUP), ptc(), ntc()));
    }


  @Test
  void prefixOverlapOutputs_isError() {
    // ["b"] vs ["b","child"]
    assertThrows(IllegalArgumentException.class, () -> validate(set(B, B_CHILD), ptc(), ntc()));
  }

  @Test
  void noProducers_isValid() {
    // Only a consumer; validator checks producers only
    assertDoesNotThrow(() -> validate(set(Z_CONSUMER), ptc(), ntc()));
  }
}
