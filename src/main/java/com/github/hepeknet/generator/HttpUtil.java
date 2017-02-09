package com.github.hepeknet.generator;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HttpUtil {

	private static final Logger LOG = LoggerFactory.getLogger(HttpUtil.class);

	public static String executeGetRequest(String url) {
		try {
			LOG.debug("Executing GET request to {}", url);
			final CloseableHttpClient httpclient = HttpClients.createDefault();
			try {
				final HttpGet httpget = new HttpGet(url);
				final ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

					@Override
					public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
						final int status = response.getStatusLine().getStatusCode();
						if (status >= 200 && status < 300) {
							final HttpEntity entity = response.getEntity();
							return entity != null ? EntityUtils.toString(entity) : null;
						} else {
							throw new ClientProtocolException("Unexpected response status: " + status);
						}
					}
				};
				final String responseBody = httpclient.execute(httpget, responseHandler);
				LOG.debug("Successfully executed GET request to {} and response is {}", url, responseBody);
				return responseBody;
			} finally {
				httpclient.close();
			}
		} catch (final IOException ie) {
			throw new RuntimeException(ie);
		}
	}

}
