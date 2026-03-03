package com.uber.data.kafka.connect.aakafkaoffsetmgmt;

import org.apache.kafka.common.TopicPartition;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.Optional;

import io.opentracing.contrib.apache.http.client.TracingHttpClientBuilder;
import io.opentracing.util.GlobalTracer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class OffsetSnapshotClient implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(OffsetSnapshotClient.class);

  private static final String getUrlFormatString = "http://127.0.0.1:18668/snapshot/%s/%s/%s/%d/%d?srcdst=src";
  private static final int DEFAULT_MAX_CONNECTION_PER_ROUTE = 8;
  private static final String DEFAULT_UBER_SOURCE_VALUE = "ureplicator2-worker";
  private static final String X_UBER_SOURCE_HEADER = "x-uber-source";

  private final String snapshotPostUrl;
  private final CloseableHttpClient httpClient;
  private final RequestConfig requestConfig;
//  protected final Meter snapshotReportCount = new Meter();
//  protected final Meter snapshotReportFailure = new Meter();
//  protected final Timer snapshotReportResponse = new Timer();
//  protected final Meter snapshotFetchCount = new Meter();
//  protected final Meter snapshotFetchFailure = new Meter();
//  protected final Timer snapshotFetchResponse = new Timer();

  public OffsetSnapshotClient(String snapshotPostUrl, int requestTimeOutInMs) {
    this(snapshotPostUrl, requestTimeOutInMs, buildDefaultConnectionManager());
  }

  private static HttpClientConnectionManager buildDefaultConnectionManager() {
    PoolingHttpClientConnectionManager result = new PoolingHttpClientConnectionManager();
    result.setDefaultMaxPerRoute(DEFAULT_MAX_CONNECTION_PER_ROUTE);
    return result;
  }

  @VisibleForTesting
  OffsetSnapshotClient(
          String snapshotPostUrl,
          int requestTimeOutInMs,
          HttpClientConnectionManager connMgrForHttpClient
  ) {
    this.snapshotPostUrl = snapshotPostUrl;
    // requestConfig is immutable. These three timeouts are for
    // 1. getting connection from connection manager;
    // 2. establishing connection with server;
    // 3. getting next data snippet from server.
    this.requestConfig = RequestConfig.custom()
        .setConnectionRequestTimeout(requestTimeOutInMs)
        .setConnectTimeout(requestTimeOutInMs)
        .setSocketTimeout(requestTimeOutInMs)
        .build();

    TracingHttpClientBuilder builder = TracingHttpClientBuilder.create()
        .withTracer(GlobalTracer.get());
    this.httpClient = builder.setConnectionManager(connMgrForHttpClient).build();
//    KafkaUReplicatorMetricsReporter.get().registerMetric("snapshot.report-count", snapshotReportCount);
//    KafkaUReplicatorMetricsReporter.get().registerMetric("snapshot.report-failure", snapshotReportFailure);
//    KafkaUReplicatorMetricsReporter.get().registerMetric("snapshot.report-time", snapshotReportResponse);
//    KafkaUReplicatorMetricsReporter.get().registerMetric("snapshot.fetch-count", snapshotFetchCount);
//    KafkaUReplicatorMetricsReporter.get().registerMetric("snapshot.fetch-failure", snapshotFetchFailure);
//    KafkaUReplicatorMetricsReporter.get().registerMetric("snapshot.fetch-time", snapshotFetchResponse);
  }

  public Optional<Long> convertOffsetBySourceCluster(
          TopicPartition topicPartition,
          String srcCluster,
          String dstCluster,
          long offsetInSrcCluster
  ) {
//    snapshotFetchCount.mark();
    // Rule is that the last 3 params are all for the same cluster
    String urlString = String.format(getUrlFormatString, topicPartition.topic(),
        srcCluster, dstCluster, topicPartition.partition(), offsetInSrcCluster);
    log.info("Converting offset via url {}", urlString);
    HttpGet httpGet = new HttpGet(urlString);
    httpGet.setConfig(requestConfig);
    httpGet.addHeader(X_UBER_SOURCE_HEADER, DEFAULT_UBER_SOURCE_VALUE);
//    return snapshotFetchResponse.timeSupplier(() -> {
      try {
        CloseableHttpResponse resp = httpClient.execute(httpGet);
        String retVal = IOUtils.toString(resp.getEntity().getContent(), UTF_8);
        resp.close();
        JSONObject parsed = JSON.parseObject(retVal);
        JSONArray offsets = parsed.getJSONArray(String.valueOf(topicPartition.partition()));
        if (offsets != null && !offsets.isEmpty()) {
          Long offset = offsets.getLong(0);
          return Optional.of(offset);
        } else {
          log.warn("Failed to fetch offset from response {}", retVal);
        }
      }catch(IOException ex) {
//        snapshotReportFailure.mark();
        log.error("Failed to GET via {}", urlString, ex);
      }
      return Optional.empty();
//    });
  }

  private static boolean isSuccessStatus(final int status) {
    // 2xx indicates success
    return status >= 200 && status < 300;
  }

  public void reportSnapshot(
          Map<String, PartitionOffsetMap> topicPartitionOffsetMap,
          String srcClusterName,
          String dstClusterName
  ) {
    long startInMs = System.currentTimeMillis();
    log.info("Start to report offset snapshot at={}", startInMs);
    HttpPost httpPost = createHttpPost(topicPartitionOffsetMap, srcClusterName, dstClusterName);
    if (httpPost == null) {
      log.info("Skipped posting offset because there is no data");
    } else {
//      snapshotReportCount.mark();
//      snapshotReportResponse.time(() -> {
        try {
          int respStatus = httpClient.execute(httpPost, createResponseCodeExtractor());
          if (isSuccessStatus(respStatus)) {
            long endInMs = System.currentTimeMillis();
            log.info("Report offset snapshot done at={} tookMs={}", endInMs, endInMs - startInMs);
          } else {
//            snapshotReportFailure.mark();
            log.info("Got error respStatus={} to post offset snapshot", respStatus);
          }
        } catch (SocketTimeoutException e) {
//          snapshotReportFailure.mark();
          log.warn("Got timeout exception to post offset snapshot, {}", e.getMessage());
        } catch (Throwable t) {
//          snapshotReportFailure.mark();
          log.warn("Got unknown exception to post offset snapshot", t);
        }
//      });
    }
  }

  public void markSnapshotReportFailure() {
//    snapshotReportFailure.mark();
  }

  private HttpPost createHttpPost(
          Map<String, PartitionOffsetMap> topicPartitionOffsetMap,
          String srcClusterName,
          String dstClusterName
  ) {
    if (topicPartitionOffsetMap.isEmpty()) {
      return null;
    }
    JSONArray postBody = new JSONArray();
    for (Map.Entry<String, PartitionOffsetMap> entry : topicPartitionOffsetMap.entrySet()) {
      String topicName = entry.getKey();
      Map<Pair<Integer, Integer>, Pair<Long, Long>> partitionOffsetMap = entry.getValue().getPartitionOffsetMap();
      for (Map.Entry<Pair<Integer, Integer>, Pair<Long, Long>> mapping : partitionOffsetMap.entrySet()) {
        JSONObject mappingInJson = new JSONObject();
        mappingInJson.put("topicName", topicName);
        mappingInJson.put("srcClusterName", srcClusterName);
        mappingInJson.put("dstClusterName", dstClusterName);
        mappingInJson.put("dstPartitionId", mapping.getKey().getLeft());
        mappingInJson.put("dstOffset", mapping.getValue().getLeft());

        JSONObject srcOffsetInJson = new JSONObject();
        srcOffsetInJson.put("" + mapping.getKey().getRight(), mapping.getValue().getRight());
        mappingInJson.put("srcPartitionOffsets", srcOffsetInJson);
        postBody.add(mappingInJson);
      }
    }

    // there is no strict synchronization, so few offset mapping might be dropped
    // this is negligible, since we overwrite offset mappings during post interval anyway
    topicPartitionOffsetMap.clear();

    HttpPost httpPost = new HttpPost(snapshotPostUrl);
    // since the post interval is pretty long, thus close conn to release resource
    httpPost.addHeader("Connection", "close");
    httpPost.addHeader("Content-Type", "application/json; charset=UTF-8");
    httpPost.setEntity(new ByteArrayEntity(postBody.toString().getBytes(Charsets.UTF_8)));
    httpPost.setConfig(requestConfig);
    return httpPost;
  }

  private static ResponseHandler<Integer> createResponseCodeExtractor() {
    return response -> response.getStatusLine().getStatusCode();
  }

  public void close() throws IOException {
    httpClient.close();
  }
}
