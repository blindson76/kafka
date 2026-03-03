package com.uber.data.kafka.connect.rest.resources;

import org.apache.kafka.connect.health.ConnectClusterState;

import com.uber.data.kafka.connect.rest.entities.TaskAssignmentEntity;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignedPartition;
import com.uber.data.kafka.connect.ureplicator3.coordination.AssignmentStores;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import static com.uber.data.kafka.connect.ureplicator3.UReplicator3Config.SOURCE_CLUSTER_CONFIG;
import static com.uber.data.kafka.connect.ureplicator3.UReplicator3Config.TARGET_CLUSTER_CONFIG;

@Path("/uber/assignments")
@Produces(MediaType.APPLICATION_JSON)
public class AssignmentsResource {

  private static final Logger log = LoggerFactory.getLogger(AssignmentsResource.class);

  private final ConnectClusterState clusterState;

  @Inject
  public AssignmentsResource(ConnectClusterState clusterState) {
    this.clusterState = clusterState;
  }

  @Path("/{connector}/all")
  @GET
  public Response getConnectorAssignments(@PathParam("connector") String connector) {
    Map<String, String> connectorConfig = clusterState.connectorConfig(connector);
    if (connectorConfig == null) {
      return Response.status(Status.NOT_FOUND)
              .entity("Connector '" + connector + " does not appear to exist")
              .build();
    }

    String srcKafka = connectorConfig.get(SOURCE_CLUSTER_CONFIG);
    if (srcKafka == null) {
      return Response.serverError()
              .entity("Could not determine source cluster for connector '" + connector + "'")
              .build();
    }

    String dstKafka = connectorConfig.get(TARGET_CLUSTER_CONFIG);
    if (dstKafka == null) {
      return Response.serverError()
              .entity("Could not determine target cluster for connector '" + connector + "'")
              .build();
    }

    Map<Integer, ? extends Collection<AssignedPartition>> taskAssignments =
            AssignmentStores.get().getAssignments(connector, srcKafka, dstKafka);

    int highestTask = Collections.max(taskAssignments.keySet());

    List<TaskAssignmentEntity> body = IntStream.rangeClosed(0, highestTask)
            .mapToObj(i -> {
              Collection<AssignedPartition> assignment = taskAssignments.get(i);
              if (assignment == null) {
                assignment = Set.of();
              }
              return TaskAssignmentEntity.of(connector, i, assignment);
            }).toList();

    return Response.ok(body).build();
  }

}
