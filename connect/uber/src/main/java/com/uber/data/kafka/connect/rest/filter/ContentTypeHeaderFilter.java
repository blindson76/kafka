package com.uber.data.kafka.connect.rest.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.MediaType;

import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

@PreMatching
public class ContentTypeHeaderFilter implements ContainerRequestFilter {

  private static final Logger log = LoggerFactory.getLogger(ContentTypeHeaderFilter.class);

  private static final String SKIP_PARAM = "keepContentType";

  // Media types that we override unless explicitly instructed not to via the skip URL param
  // TODO: We could technically make this configurable but it's probably not worth the effort
  private static final Set<MediaType> STUPID_MEDIA_TYPES = Set.of(
          APPLICATION_FORM_URLENCODED_TYPE // Automatically injected by curl (this fucking shit took way too long to debug)
  );

  @Override
  public void filter(ContainerRequestContext containerRequestContext) {
    if (containerRequestContext.getUriInfo().getQueryParameters().containsKey(SKIP_PARAM)) {
      log.trace("Skipping {} header injection since {} URL param is specified", CONTENT_TYPE, SKIP_PARAM);
      return;
    }

    if (!containerRequestContext.hasEntity()) {
      log.trace("Skipping {} header injection since request has no body", CONTENT_TYPE);
      return;
    }

    MediaType requestMediaType = containerRequestContext.getMediaType();
    if (requestMediaType == null) {
      log.trace("Request does not have media type; will inject {} header", CONTENT_TYPE);
    } else if (STUPID_MEDIA_TYPES.contains(requestMediaType)) {
      log.trace("Request has stupid media type {}; will override by injecting {} header", requestMediaType, CONTENT_TYPE);
    } else {
      log.trace("Skipping {} header injection since request already has non-stupid media type {}", CONTENT_TYPE, requestMediaType);
      return;
    }

    log.debug("Adding {} header with value {} to request", CONTENT_TYPE, APPLICATION_JSON);
    containerRequestContext.getHeaders().putSingle(CONTENT_TYPE, APPLICATION_JSON);
  }

}
