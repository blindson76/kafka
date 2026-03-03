package com.uber.data.kafka.connect.rest.resources;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.health.ConnectorHealth;
import org.apache.kafka.connect.health.TaskState;
import org.apache.kafka.connect.util.ConnectorTaskId;

import com.uber.data.kafka.connect.common.ReplicatedTopicPartition;
import com.uber.data.kafka.connect.rest.entities.CachedWorkloadEntity;
import com.uber.data.kafka.connect.rest.entities.PartitionWorkloadEntity;
import com.uber.data.kafka.connect.rest.entities.TaskWorkloadEntity;
import com.uber.data.kafka.connect.rest.entities.WorkerWorkloadEntity;
import com.uber.data.kafka.connect.rest.entities.WorkloadEntity;
import com.uber.data.kafka.connect.workload.common.CachedWorkload;
import com.uber.data.kafka.connect.workload.common.Workload;
import com.uber.data.kafka.connect.workload.kafka.KafkaBasedWorkloadProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import static com.uber.data.kafka.connect.utils.UberConnectUtils.assignedPartitions;
import static com.uber.data.kafka.connect.utils.UberConnectUtils.awaitAll;
import static com.uber.data.kafka.connect.utils.UberConnectUtils.createErrorMessage;

@Path("/uber/workloads")
@Produces(MediaType.APPLICATION_JSON)
public class WorkloadsResource {

    private static final Logger log = LoggerFactory.getLogger(WorkloadsResource.class);

    private final ConnectClusterState clusterState;

    @Inject
    public WorkloadsResource(ConnectClusterState clusterState) {
        this.clusterState = clusterState;
    }

    @GET
    @Path("/topics/all")
    public List<PartitionWorkloadEntity> getAllTopicWorkloads(
            @QueryParam("src") String srcKafka,
            @QueryParam("dst") String dstKafka,
            @QueryParam("topic") String topic,
            @QueryParam("refresh") @DefaultValue("false") boolean refresh
    ) {
        if (refresh)
            KafkaBasedWorkloadProvider.refreshWorkloadCache();

        return KafkaBasedWorkloadProvider.getAllWorkloads().entrySet().stream()
                .filter(e -> matches(e.getKey(), srcKafka, dstKafka, topic))
                .map(e -> PartitionWorkloadEntity.of(e.getKey(), e.getValue()))
                .sorted(PartitionWorkloadEntity.lexicographicComparator())
                .collect(Collectors.toList());
    }

    @GET
    @Path("/topics/historical/{src}/{dst}/{topic}/{partition}")
    public Response getHistoricalWorkloads(
            @PathParam("src") String srcKafka,
            @PathParam("dst") String dstKafka,
            @PathParam("topic") String topic,
            @PathParam("partition") int partition,
            @QueryParam("refresh") @DefaultValue("false") boolean refresh
    ) {
        if (refresh)
            KafkaBasedWorkloadProvider.refreshWorkloadCache();

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        List<CachedWorkload> cachedWorkloads = KafkaBasedWorkloadProvider.getHistoricalWorkloads(srcKafka, dstKafka, topicPartition);
        if (cachedWorkloads == null)
            return Response.status(Status.NOT_FOUND).entity("no workloads found for the requested topic").build();

        List<CachedWorkloadEntity> body = cachedWorkloads.stream()
                .map(CachedWorkloadEntity::of)
                .toList();
        return Response.ok(body).build();
    }

    @POST
    @Path("/topics/overrides/{src}/{dst}/{topic}/{partition}")
    public Response publishTopicWorkload(
            @QueryParam("awaitAck") @DefaultValue("true") boolean awaitAck,
            @PathParam("src") String srcKafka,
            @PathParam("dst") String dstKafka,
            @PathParam("topic") String topic,
            @PathParam("partition") int partition,
            WorkloadEntity workload
    ) {
        return publishWorkload(srcKafka, dstKafka, topic, partition, workload.workload(), awaitAck);
    }

    @DELETE
    @Path("/topics/overrides/{src}/{dst}/{topic}")
    public Response deleteTopicWorkloads(
        @QueryParam("awaitAck") @DefaultValue("true") boolean awaitAck,
        @QueryParam("partitions") int partitions,
        @PathParam("src") String srcKafka,
        @PathParam("dst") String dstKafka,
        @PathParam("topic") String topic
    ) {
        List<? extends Future<?>> produceFutures = IntStream.range(0, partitions)
            .mapToObj(partition -> KafkaBasedWorkloadProvider.publishWorkload(
                srcKafka,
                dstKafka,
                new TopicPartition(topic, partition),
                null
            )).toList();

        if (awaitAck) {
            try {
                awaitAll(produceFutures, Duration.ofMinutes(1));
            } catch (InterruptedException e) {
                return overrideErrorResponse(e);
            } catch (TimeoutException e) {
                return overrideErrorResponse(e);
            } catch (ExecutionException e) {
                return overrideErrorResponse(e);
            }
        } else {
            log.debug("Not awaiting producer ack for workload override on {} partitions of topic {} in pipeline @{}@{}",
                partitions, topic, srcKafka, dstKafka
            );
        }

        return Response.noContent().build();
    }

    @DELETE
    @Path("/topics/overrides/{src}/{dst}/{topic}/{partition}")
    public Response deleteTopicWorkload(
            @QueryParam("awaitAck") @DefaultValue("true") boolean awaitAck,
            @PathParam("src") String srcKafka,
            @PathParam("dst") String dstKafka,
            @PathParam("topic") String topic,
            @PathParam("partition") int partition
    ) {
        return publishWorkload(srcKafka, dstKafka, topic, partition, null, awaitAck);
    }

    private Response publishWorkload(
            String srcKafka,
            String dstKafka,
            String topic,
            int partition,
            Workload workload,
            boolean awaitAck
    ) {
        Future<?> produceFuture = KafkaBasedWorkloadProvider.publishWorkload(
                srcKafka,
                dstKafka,
                new TopicPartition(topic, partition),
                workload
        );
        if (awaitAck) {
            try {
                produceFuture.get(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                return overrideErrorResponse(e);
            } catch (TimeoutException e) {
                return overrideErrorResponse(e);
            } catch (ExecutionException e) {
                return overrideErrorResponse(e);
            }
        } else {
            ReplicatedTopicPartition loggedPartition = new ReplicatedTopicPartition(srcKafka, dstKafka, topic, partition);
            log.debug("Not awaiting producer ack for workload override on {}", loggedPartition);
        }

        return Response.noContent().build();
    }

    @GET
    @Path("/workers/all")
    public List<WorkerWorkloadEntity> getAllWorkerWorkloads(@QueryParam("refresh") @DefaultValue("false") boolean refresh) {
        Collection<String> connectors = clusterState.connectors();
        Map<String, Set<ConnectorTaskId>> workerTasks = new HashMap<>();

        for (String connector : connectors) {
            ConnectorHealth connectorHealth = clusterState.connectorHealth(connector);
            if (connectorHealth == null) {
                // The connector may have just been deleted
                continue;
            }
            for (TaskState taskState : connectorHealth.tasksState().values()) {
                workerTasks.computeIfAbsent(taskState.workerId(), k -> new HashSet<>())
                        .add(new ConnectorTaskId(connector, taskState.taskId()));
            }
        }

        if (refresh)
            KafkaBasedWorkloadProvider.refreshWorkloadCache();
        Map<ReplicatedTopicPartition, Workload> workloads = KafkaBasedWorkloadProvider.getAllWorkloads();

        Map<String, Map<ConnectorTaskId, Workload>> workerTaskWorkloads = new HashMap<>();
        Map<String, Map<String, String>> connectorConfigs = new HashMap<>();
        workerTasks.forEach((worker, tasks) ->
            tasks.forEach(task -> {
                Map<String, String> connectorConfig = connectorConfigs.computeIfAbsent(task.connector(), clusterState::connectorConfig);
                if (connectorConfig == null) {
                    // The connector may have just been deleted
                    return;
                }

                Set<ReplicatedTopicPartition> taskPartitions = assignedPartitions(task.connector(), task.task(), connectorConfig);

                if (taskPartitions == null) {
                    return;
                }

                for (ReplicatedTopicPartition replicatedTopicPartition : taskPartitions) {
                    Workload workload = workloads.get(replicatedTopicPartition);
                    if (workload == null) {
                        continue;
                    }
                    workerTaskWorkloads.computeIfAbsent(
                            worker,
                            k -> new TreeMap<>(Comparator.comparing(ConnectorTaskId::connector).thenComparing(ConnectorTaskId::task))
                        ).compute(
                            task,
                            (k, existingWorkload) -> workload.plus(existingWorkload)
                        );
                }
            })
        );

        return workerTaskWorkloads.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> {
                    String worker = e.getKey();
                    Map<ConnectorTaskId, Workload> taskWorkloads = e.getValue();

                    List<TaskWorkloadEntity> taskWorkloadEntities = new ArrayList<>();
                    double totalByteRate = 0;
                    double totalMessageRate = 0;

                    // TODO: We might want to order this somehow (probably lexicographically by connector name and then task ID)
                    for (Map.Entry<ConnectorTaskId, Workload> taskWorkloadEntry : taskWorkloads.entrySet()) {
                        totalByteRate += taskWorkloadEntry.getValue().getBytesPerSecond();
                        totalMessageRate += taskWorkloadEntry.getValue().getMessagesPerSecond();;

                        TaskWorkloadEntity taskWorkloadEntity = new TaskWorkloadEntity(
                                taskWorkloadEntry.getKey().connector(),
                                taskWorkloadEntry.getKey().task(),
                                WorkloadEntity.of(taskWorkloadEntry.getValue())
                        );
                        taskWorkloadEntities.add(taskWorkloadEntity);
                    }

                    WorkloadEntity totalWorkload = new WorkloadEntity(totalByteRate, totalMessageRate);
                    return new WorkerWorkloadEntity(worker, totalWorkload, taskWorkloadEntities);
                }).toList();
    }

    private boolean matches(ReplicatedTopicPartition rtp, String srcKafka, String dstKafka, String topic) {
        if (srcKafka != null && !srcKafka.equals(rtp.srcKafka()))
            return false;

        if (dstKafka != null && !dstKafka.equals(rtp.dstKafka()))
            return false;

        if (topic != null && !topic.equals(rtp.topic()))
            return false;

        return true;
    }

    private Response overrideErrorResponse(InterruptedException e) {
        return Response.serverError()
        .entity(createErrorMessage("Interrupted while waiting for producer ack", e))
        .build();
    }

    private Response overrideErrorResponse(TimeoutException e) {
        return Response.serverError()
        .entity(createErrorMessage("Timed out waiting for producer ack", e))
        .build();
    }

    private Response overrideErrorResponse(ExecutionException e) {
        return Response.serverError()
        .entity(createErrorMessage("Failed to publish workload override", e))
        .build();
    }

}
