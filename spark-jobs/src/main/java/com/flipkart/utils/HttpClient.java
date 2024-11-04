package com.flipkart.utils;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.FluentCaseInsensitiveStringsMap;
import com.ning.http.client.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by shivang.b on 12/16/15.
 */
public enum HttpClient {
    INSTANCE;

    private final AsyncHttpClient asyncHttpClient;

    private HttpClient() {
        asyncHttpClient = new AsyncHttpClient();
    }

    public Response executeGet(String url) throws InterruptedException, ExecutionException, IOException {
        final Map<String, String> headers = new HashMap<String, String>();
        final Map<String, String> params = new HashMap<String, String>();
        return executeGet(url, params, headers);

    }

    public Response executeGet(String url, Map<String, String> params, Map<String, String> headers)
            throws InterruptedException, ExecutionException, IOException {
        final AsyncHttpClient.BoundRequestBuilder requestBuilder = asyncHttpClient.prepareGet(url);
        if(params!= null) {
            for(Map.Entry<String, String> entry: params.entrySet()) {
                requestBuilder.addQueryParameter(entry.getKey(), entry.getValue());
            }
        }
        if(headers != null) {
            for(Map.Entry<String, String> entry: headers.entrySet()) {
                requestBuilder.addHeader(entry.getKey(), entry.getValue());
            }
        }
        return requestBuilder.execute().get();
    }

    public Response executePost(String url, String body, Map<String, String> headers)
            throws InterruptedException, ExecutionException, IOException {

        final FluentCaseInsensitiveStringsMap map = new FluentCaseInsensitiveStringsMap();
        if(headers != null) {
            for(Map.Entry<String, String> entry: headers.entrySet()) {
                map.add(entry.getKey(), entry.getValue());
            }
        }
        return asyncHttpClient.preparePost(url).setBody(body).setHeaders(map).execute().get();
    }


}

