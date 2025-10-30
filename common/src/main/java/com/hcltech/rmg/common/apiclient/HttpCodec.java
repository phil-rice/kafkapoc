package com.hcltech.rmg.common.apiclient;

import java.net.URI;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.time.Duration;

/** A codec that can build a request using a per-call paramName. */
public interface HttpCodec<In, Out> {
    HttpRequest buildRequest(URI baseUrl, Duration readTimeout, String paramName, String corrId,In in);
    Out decode(int statusCode, String body, HttpHeaders headers);
}
