package com.hcltech.rmg.common.function;

public interface Callback<T> {
  void success(T value);
  void failure(Throwable error);
}

