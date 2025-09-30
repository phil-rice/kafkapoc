package com.hcltech.rmg.common;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

public record ModuleNameWithResult<T>(String moduleName,T result)  {
}
