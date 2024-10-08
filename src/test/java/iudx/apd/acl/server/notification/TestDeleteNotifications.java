package iudx.apd.acl.server.notification;

import static iudx.apd.acl.server.Utility.*;
import static iudx.apd.acl.server.apiserver.util.Constants.*;
import static iudx.apd.acl.server.common.HttpStatusCode.*;
import static iudx.apd.acl.server.notification.util.Constants.GET_REQUEST;
import static iudx.apd.acl.server.notification.util.Constants.WITHDRAW_REQUEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.Tuple;
import iudx.apd.acl.server.Utility;
import iudx.apd.acl.server.apiserver.util.User;
import iudx.apd.acl.server.common.HttpStatusCode;
import iudx.apd.acl.server.common.ResponseUrn;
import iudx.apd.acl.server.policy.PostgresService;
import java.time.LocalDateTime;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith({VertxExtension.class, MockitoExtension.class})
public class TestDeleteNotifications {
  @Container static PostgreSQLContainer container = new PostgreSQLContainer<>("postgres:12.11");
  private static DeleteNotification deleteNotification;
  private static Utility utility;
  private static User consumer;

  @BeforeAll
  public static void setUp(VertxTestContext vertxTestContext) {
    container.start();
    utility = new Utility();
    PostgresService pgService = utility.setUp(container);

    utility
        .testInsert()
        .onComplete(
            handler -> {
              if (handler.succeeded()) {
                consumer = getConsumer();
                deleteNotification = new DeleteNotification(pgService);
                vertxTestContext.completeNow();
              } else {
                vertxTestContext.failNow("Failed to set up");
              }
            });
  }

  public static User getConsumer() {
    JsonObject jsonObject =
        new JsonObject()
            .put(USER_ID, utility.getConsumerId())
            .put(USER_ROLE, "consumer")
            .put(RS_SERVER_URL, "rs.iudx.io")
            .put(EMAIL_ID, utility.getConsumerEmailId())
            .put(FIRST_NAME, utility.getConsumerFirstName())
            .put(LAST_NAME, utility.getConsumerLastName());
    return new User(jsonObject);
  }

  public static User getOwner() {
    JsonObject jsonObject =
        new JsonObject()
            .put(USER_ID, utility.getOwnerId())
            .put(USER_ROLE, "provider")
            .put(RS_SERVER_URL, "rs.iudx.io")
            .put(EMAIL_ID, utility.getOwnerEmailId())
            .put(FIRST_NAME, utility.getOwnerFirstName())
            .put(LAST_NAME, utility.getOwnerLastName());
    return new User(jsonObject);
  }

  @Test
  @DisplayName("Test initiateDeleteNotification: Success")
  public void testInitiateDeleteNotification(VertxTestContext vertxTestContext) {
    deleteNotification
        .initiateDeleteNotification(new JsonObject().put("id", utility.getRequestId()), consumer)
        .onComplete(
            handler -> {
              if (handler.succeeded()) {
                assertEquals(ResponseUrn.SUCCESS_URN.getUrn(), handler.result().getString(TYPE));
                assertEquals(
                    ResponseUrn.SUCCESS_URN.getMessage(), handler.result().getString(TITLE));
                assertEquals("Request deleted successfully", handler.result().getString(DETAIL));
                vertxTestContext.completeNow();

              } else {
                vertxTestContext.failNow("Failed");
              }
            });
  }

  @Test
  @DisplayName("Test initiateDeleteNotification with resource server url mismatch")
  public void testInitiateDeleteNotificationWithRsUrlMismatch(VertxTestContext vertxTestContext) {
    JsonObject jsonObject =
        new JsonObject()
            .put(USER_ID, utility.getConsumerId())
            .put(USER_ROLE, "consumer")
            .put(RS_SERVER_URL, "some.dummy.url")
            .put(EMAIL_ID, utility.getConsumerEmailId())
            .put(FIRST_NAME, utility.getConsumerFirstName())
            .put(LAST_NAME, utility.getConsumerLastName());
    User consumer = new User(jsonObject);
    deleteNotification
        .initiateDeleteNotification(new JsonObject().put("id", utility.getRequestId()), consumer)
        .onComplete(
            handler -> {
              if (handler.failed()) {
                JsonObject result = new JsonObject(handler.cause().getMessage());
                assertEquals(HttpStatusCode.NOT_FOUND.getValue(), result.getInteger(TYPE));
                assertEquals(ResponseUrn.RESOURCE_NOT_FOUND_URN.getUrn(), result.getString(TITLE));
                assertEquals(
                    "Request could not be withdrawn, as it is not found",
                    result.getString(DETAIL));
                vertxTestContext.completeNow();

              } else {
                vertxTestContext.failNow(
                    "Succeed when the resource server url of the consumer and resource did not match");
              }
            });
  }

  @Test
  @DisplayName("Test initiateDeleteNotification with invalid user")
  public void testInitiateDeleteNotification4InvalidUser(VertxTestContext vertxTestContext) {
    deleteNotification
        .initiateDeleteNotification(new JsonObject().put("id", utility.getRequestId()), getOwner())
        .onComplete(
            handler -> {
              if (handler.failed()) {
                JsonObject result = new JsonObject(handler.cause().getMessage());
                assertEquals(HttpStatusCode.NOT_FOUND.getValue(), result.getInteger(TYPE));
                assertEquals(ResponseUrn.RESOURCE_NOT_FOUND_URN.getUrn(), result.getString(TITLE));
                assertEquals(
                    "Request could not be withdrawn, as it is not found",
                    result.getString(DETAIL));
                vertxTestContext.completeNow();

              } else {
                vertxTestContext.failNow("Failed");
              }
            });
  }

  @ParameterizedTest
  @DisplayName("Test initiateDeleteNotification with null user")
  @NullSource
  public void testInitiateDeleteNotification4NullUser(
      User user, VertxTestContext vertxTestContext) {
    assertThrows(
        NullPointerException.class,
        () ->
            deleteNotification.initiateDeleteNotification(
                new JsonObject().put("id", utility.getRequestId()), user));
    vertxTestContext.completeNow();
  }

  @ParameterizedTest
  @DisplayName("Test initiateDeleteNotification with null notification")
  @NullSource
  public void testInitiateDeleteNotification4NullNotification(
      JsonObject notification, VertxTestContext vertxTestContext) {
    assertThrows(
        NullPointerException.class,
        () -> deleteNotification.initiateDeleteNotification(notification, getConsumer()));
    vertxTestContext.completeNow();
  }

  @Test
  @DisplayName("Test initiateDeleteNotification with an invalid notification id")
  public void testInitiateDeleteNotification4InvalidRequestId(VertxTestContext vertxTestContext) {

    deleteNotification
        .initiateDeleteNotification(new JsonObject().put("id", utility.getOwnerId()), consumer)
        .onComplete(
            handler -> {
              if (handler.succeeded()) {
                vertxTestContext.failNow("Succeeded with an invalid Request ID");
              } else {
                JsonObject result = new JsonObject(handler.cause().getMessage());
                assertEquals(HttpStatusCode.NOT_FOUND.getValue(), result.getInteger(TYPE));
                assertEquals(ResponseUrn.RESOURCE_NOT_FOUND_URN.getUrn(), result.getString(TITLE));
                assertEquals(
                    "Request could not be withdrawn, as it is not found", result.getString(DETAIL));
                vertxTestContext.completeNow();
              }
            });
  }

  @Test
  @DisplayName("Test initiateDeleteNotification with invalid User")
  public void testInitiateDeleteNotification4UserId(VertxTestContext vertxTestContext) {
    deleteNotification
        .initiateDeleteNotification(new JsonObject().put("id", utility.getRequestId()), getOwner())
        .onComplete(
            handler -> {
              if (handler.succeeded()) {
                vertxTestContext.failNow("Succeeded with an invalid user id");
              } else {
                JsonObject result = new JsonObject(handler.cause().getMessage());
                assertEquals(NOT_FOUND.getValue(), result.getInteger(TYPE));
                assertEquals(ResponseUrn.RESOURCE_NOT_FOUND_URN.getUrn(), result.getString(TITLE));
                assertEquals(
                    "Request could not be withdrawn, as it is not found",
                    result.getString(DETAIL));
                vertxTestContext.completeNow();
              }
            });
  }

  @Test
  @DisplayName("Test executeQuery method with invalid tuple")
  public void testExecuteQueryWithInvalidTuple(VertxTestContext vertxTestContext) {
    deleteNotification.executeQuery(
        GET_REQUEST,
        Tuple.tuple(),
        handler -> {
          if (handler.succeeded()) {
            vertxTestContext.failNow("Succeeded for invalid tuple");
          } else {
            JsonObject result = new JsonObject(handler.cause().getMessage());
            assertEquals(500, result.getInteger(TYPE));
            assertEquals(ResponseUrn.DB_ERROR_URN.getUrn(), result.getString(TITLE));
            assertEquals("Failure while executing query", result.getString(DETAIL));
            vertxTestContext.completeNow();
          }
        });
  }

  @Test
  @DisplayName("Test initiateDeleteNotification method when notification has been WITHDRAWN")
  public void testInitiateDeleteNotification4WithdrawnRequest(VertxTestContext vertxTestContext) {
    UUID requestId = UUID.randomUUID();

    Tuple tuple =
        Tuple.of(
            requestId,
            utility.getConsumerId(),
            utility.getResourceId(),
            utility.getOwnerId(),
            "WITHDRAWN",
            LocalDateTime.of(2023, 1, 1, 1, 1, 1, 1),
            LocalDateTime.of(2024, 1, 1, 1, 1, 1, 1),
            utility.getConstraints());
    utility
        .executeQuery(tuple, INSERT_INTO_REQUEST_TABLE)
        .onComplete(
            insertionHandler -> {
              if (insertionHandler.succeeded()) {
                deleteNotification
                    .initiateDeleteNotification(new JsonObject().put("id", requestId), consumer)
                    .onComplete(
                        handler -> {
                          if (handler.succeeded()) {
                            vertxTestContext.failNow(
                                "Succeeded for previously deleted notification");
                          } else {
                            JsonObject result = new JsonObject(handler.cause().getMessage());
                            assertEquals(BAD_REQUEST.getValue(), result.getInteger(TYPE));
                            assertEquals(
                                ResponseUrn.BAD_REQUEST_URN.getUrn(), result.getString(TITLE));
                            assertEquals(
                                "Request could not be withdrawn, as it is not in pending status",
                                result.getString(DETAIL));
                            vertxTestContext.completeNow();
                          }
                        });
              } else {
                vertxTestContext.failNow(insertionHandler.cause().getMessage());
              }
            });
  }

  /*    @Test
  @DisplayName("Test initiateDeleteNotification method when notification is expired")
  public void testInitiateDeleteNotification4ExpiredRequest(VertxTestContext vertxTestContext)
  {
      UUID notification = generateRandomUuid();
      UUID consumerId = generateRandomUuid();
      UUID ownerId = generateRandomUuid();
      UUID resourceId = utility.getResourceId();
      String consumerEmailId = generateRandomEmailId();
      String ownerEmailId = generateRandomEmailId();
      String consumerFirstName = generateRandomString();
      String consumerLastName = generateRandomString();
      String ownerFirstName = generateRandomString();
      String ownerLastName = generateRandomString();
      LocalDateTime createdAt = LocalDateTime.of(2000, 3, 3, 10, 1);
      LocalDateTime updatedAt = LocalDateTime.of(2023, 3, 3, 10, 1);
      JsonObject jsonObject = new JsonObject()
              .put("userId", consumerId)
              .put("userRole", "consumer")
              .put("emailId", consumerEmailId)
              .put("firstName", consumerFirstName)
              .put("lastName", consumerLastName);
      User consumer =  new User(jsonObject);
      Utility utility = new Utility();
      PostgresService pgService = utility.setUp(container);
      DeleteNotification deleteNotification = new DeleteNotification(pgService);

      Tuple tuple = Tuple.of(notification, consumerId,
              resourceId, "RESOURCE_GROUP", ownerId
      , "PENDING", LocalDateTime.of(2020, 1, 1, 1, 1, 1, 1), LocalDateTime.of(2023, 1, 1, 1, 1, 1, 1), LocalDateTime.of(2023, 1, 1, 1, 1, 1, 1), "{}");
      Tuple ownerTuple =  Tuple.of(ownerId, ownerEmailId, ownerFirstName, ownerLastName, createdAt, updatedAt);

      Tuple consumerTuple = Tuple.of( consumerId, consumerEmailId, consumerFirstName, consumerLastName, createdAt, updatedAt);
      utility.executeBatchQuery(List.of(ownerTuple, consumerTuple), INSERT_INTO_USER_TABLE);
      utility.executeQuery(tuple, INSERT_INTO_REQUEST_TABLE).onComplete(insertRequestHandler -> {
          try {
              vertxTestContext.awaitCompletion(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
              throw new RuntimeException(e);
          }

          if(insertRequestHandler.succeeded())
          {
              deleteNotification.initiateDeleteNotification(new JsonObject().put("id",notification),consumer).onComplete(handler -> {
                  if(handler.succeeded())
                  {
                      vertxTestContext.failNow("Succeeded for expired notification");
                  }
                  else
                  {
                      JsonObject result = new JsonObject(handler.cause().getMessage());
                      assertEquals(BAD_REQUEST.getValue(),result.getInteger(TYPE));
                      assertEquals(BAD_REQUEST.getUrn(),result.getString(TITLE));
                      assertEquals("Request could not be withdrawn, as it is expired",result.getString(DETAIL));
                      vertxTestContext.completeNow();
                  }
              });
          }
          else
          {
              vertxTestContext.failNow("Failed");
          }
      });
  }*/
  @Test
  @DisplayName("Test executeQuery with null tuple values")
  public void testExecuteQueryWithNullTuple(VertxTestContext vertxTestContext) {
    assertThrows(
        NullPointerException.class,
        () ->
            deleteNotification.executeQuery(
                WITHDRAW_REQUEST, Tuple.of(null, null), mock(Handler.class)));
    vertxTestContext.completeNow();
  }

  @Test
  @DisplayName("Test executeQuery with invalid query")
  public void testExecuteQueryWithInvalidQuery(VertxTestContext vertxTestContext) {
    String query = "UPDATE request123 SET status='DELETED' WHERE _id = 'someDummyValue'::uuid";
    deleteNotification.executeQuery(
        query,
        Tuple.tuple(),
        handler -> {
          if (handler.succeeded()) {
            vertxTestContext.failNow("Succeeded for non-existent relation or table");
          } else {
            JsonObject result = new JsonObject(handler.cause().getMessage());
            assertEquals(500, result.getInteger(TYPE));
            assertEquals(ResponseUrn.DB_ERROR_URN.getUrn(), result.getString(TITLE));
            assertEquals("Failure while executing query", result.getString(DETAIL));
            vertxTestContext.completeNow();
          }
        });
  }
}
