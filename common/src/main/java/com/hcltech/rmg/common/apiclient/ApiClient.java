// public API (domain/port)
package com.hcltech.rmg.common.apiclient;

public interface ApiClient<In, Out> {
    Out fetch(String url, String paramName, String corrId, In in);
}
