package com.uber.data.kafka.connect.utils;

import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.m3.M3Reporter;
import com.uber.m3.util.Duration;
import com.uber.m3.util.ImmutableMap;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Singleton class to provide M3 reporter object
 */
public class M3ReporterProvider {

    public static final SocketAddress SOCKET_ADDRESS = new InetSocketAddress("localhost", 9052);
    public static final String RUNTIME_ENV_TAG = "runtime_env";
    public static final String TARGET_ENVIRONMENT = "target_environment";

    public static final Scope SCOPE = new RootScopeBuilder()
            .reporter(new M3Reporter.Builder(SOCKET_ADDRESS)
                    .includeHost(false)
                    .commonTags(buildCommonTags())
                    .build())
            .reportEvery(Duration.ofSeconds(10));

    public static final Scope HOST_SCOPE = new RootScopeBuilder()
            .reporter(new M3Reporter.Builder(SOCKET_ADDRESS)
                    .includeHost(true)
                    .commonTags(buildCommonTags())
                    .build())
            .reportEvery(Duration.ofSeconds(10));

    private static ImmutableMap<String, String> buildCommonTags() {
        ImmutableMap.Builder<String, String> tags = new ImmutableMap.Builder<>();
        tags.put(M3Reporter.SERVICE_TAG, System.getenv("UDEPLOY_SERVICE_NAME"));
        tags.put(M3Reporter.ENV_TAG, System.getenv("UBER_ENVIRONMENT"));
        tags.put(RUNTIME_ENV_TAG, System.getenv("UBER_RUNTIME_ENVIRONMENT"));
        tags.put(TARGET_ENVIRONMENT, System.getenv("UBER_RUNTIME_ENVIRONMENT"));
        return tags.build();
    }
}
