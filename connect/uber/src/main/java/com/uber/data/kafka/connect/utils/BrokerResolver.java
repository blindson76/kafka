package com.uber.data.kafka.connect.utils;


import com.uber.data.kafka.common.ClusterNameHelper;
import com.uber.data.kafka.common.ClusterNameHelper.ClusterNameParseResult;
import com.uber.data.kafka.exception.ResolverException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO: Singleton pattern and fault injection in dependent classes
public final class BrokerResolver {

    private static final Logger log = LoggerFactory.getLogger(BrokerResolver.class);

    // TODO: We should expose normalization logic so that different aliases for the same Kafka
    //       cluster get treated the same (e.g., when storing offsets or logging replication pipelines)

    public static String resolveBootstrap(String cluster) throws ResolverException {
        if (cluster == null) {
            throw new ResolverException("Cannot parse null cluster name");
        }

        ClusterNameParseResult parseResult;
        try {
            parseResult = ClusterNameHelper.parseClusterName(cluster);
        } catch (IllegalArgumentException e) {
            throw new ResolverException("Failed to parse cluster name '" + cluster + "'", e);
        }
        // TODO: There has to be a better way 💀
        return String.format(
                "kafka-%s.%s.uber.internal:9092",
                parseResult.functionalGroup,
                parseResult.region
        );
    }

}
