package iudx.apd.acl.server.auditing;

import static iudx.apd.acl.server.auditing.util.Constants.EXCHANGE_NAME;
import static iudx.apd.acl.server.auditing.util.Constants.ROUTING_KEY;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import iudx.apd.acl.server.auditing.databroker.DataBrokerService;
import iudx.apd.acl.server.auditing.util.QueryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AuditingServiceImpl implements AuditingService {
  private static final Logger LOGGER = LogManager.getLogger(AuditingServiceImpl.class);
  private DataBrokerService dataBrokerService;
  private QueryBuilder queryBuilder = new QueryBuilder();

  public AuditingServiceImpl(DataBrokerService dataBrokerService) {
    this.dataBrokerService = dataBrokerService;
  }

  /**
   * @param request JsonObject
   * @return void future
   */
  @Override
  public Future<Void> insertAuditlogIntoRmq(JsonObject request) {
    Promise<Void> promise = Promise.promise();
    JsonObject writeMessage = queryBuilder.buildMessageForRmq(request);
    dataBrokerService
        .publishMessage(EXCHANGE_NAME, ROUTING_KEY, writeMessage)
        .onSuccess(
            successHandler -> {
              promise.complete();
            })
        .onFailure(
            failureHandler -> {
              LOGGER.error(failureHandler.getMessage());
              LOGGER.debug("Fail to Published audit data into rmq");
              promise.fail(failureHandler.getMessage());
            });

    return promise.future();
  }
}
