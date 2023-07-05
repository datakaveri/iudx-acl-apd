package iudx.apd.acl.server.validation;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import iudx.apd.acl.server.apiserver.response.RestResponse;
import iudx.apd.acl.server.common.HttpStatusCode;
import iudx.apd.acl.server.validation.exceptions.DxRuntimeException;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static iudx.apd.acl.server.apiserver.util.Constants.*;

public class FailureHandler implements Handler<RoutingContext> {

  private static final Logger LOGGER = LogManager.getLogger(FailureHandler.class);

  @Override
  public void handle(RoutingContext routingContext) {
    Throwable failure = routingContext.failure();

    LOGGER.debug("Exception caught");
    if (failure instanceof DxRuntimeException) {
      DxRuntimeException exception = (DxRuntimeException) failure;
      LOGGER.error(exception.getUrn().getUrn() + " : " + exception.getMessage());
      HttpStatusCode code = HttpStatusCode.getByValue(exception.getStatusCode());

      JsonObject response =
          new RestResponse.Builder()
              .withType(exception.getUrn().getUrn())
              .withTitle(code.getDescription())
              .withMessage(code.getDescription())
              .build()
              .toJson();

      routingContext
          .response()
          .putHeader(CONTENT_TYPE, APPLICATION_JSON)
          .setStatusCode(exception.getStatusCode())
          .end(response.encode());
    }

    if (failure instanceof RuntimeException) {

      String validationErrorMessage = MSG_BAD_QUERY;
      routingContext
          .response()
          .putHeader(CONTENT_TYPE, APPLICATION_JSON)
          .setStatusCode(HttpStatus.SC_BAD_REQUEST)
          .end(validationFailureResponse(validationErrorMessage).toString());
    }

    if (routingContext.response().ended()) {
      LOGGER.debug("Already ended");
      return;
    }
    routingContext.next();
  }

  private JsonObject validationFailureResponse(String message) {
    return new JsonObject()
        .put("type", HttpStatus.SC_BAD_REQUEST)
        .put("title", "Bad Request")
        .put("detail", message);
  }
}