package com.hcltech.rmg.celimpl;

import dev.cel.common.CelException;

public class CelExecutionException extends RuntimeException {
    public CelExecutionException(CelException e) {
        super(e);
    }
}
