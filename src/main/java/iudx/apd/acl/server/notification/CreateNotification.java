package iudx.apd.acl.server.notification;

import static iudx.apd.acl.server.apiserver.util.Constants.DETAIL;
import static iudx.apd.acl.server.apiserver.util.Constants.RESULT;
import static iudx.apd.acl.server.apiserver.util.Constants.STATUS_CODE;
import static iudx.apd.acl.server.apiserver.util.Constants.TITLE;
import static iudx.apd.acl.server.apiserver.util.Constants.TYPE;
import static iudx.apd.acl.server.common.HttpStatusCode.BAD_REQUEST;
import static iudx.apd.acl.server.common.HttpStatusCode.INTERNAL_SERVER_ERROR;
import static iudx.apd.acl.server.common.ResponseUrn.BAD_REQUEST_URN;
import static iudx.apd.acl.server.common.ResponseUrn.POLICY_ALREADY_EXIST_URN;
import static iudx.apd.acl.server.notification.util.Constants.*;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import iudx.apd.acl.server.aaaService.AuthClient;
import iudx.apd.acl.server.apiserver.util.ResourceObj;
import iudx.apd.acl.server.apiserver.util.User;
import iudx.apd.acl.server.authentication.model.DxRole;
import iudx.apd.acl.server.authentication.model.UserInfo;
import iudx.apd.acl.server.common.HttpStatusCode;
import iudx.apd.acl.server.common.ResponseUrn;
import iudx.apd.acl.server.policy.CatalogueClient;
import iudx.apd.acl.server.policy.PostgresService;
import iudx.apd.acl.server.policy.util.ItemType;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateNotification {
  private static final Logger LOG = LoggerFactory.getLogger(CreateNotification.class);
  private static final String FAILURE_MESSAGE = "Request could not be created";
  private final PostgresService postgresService;
  private final CatalogueClient catalogueClient;
  private final EmailNotification emailNotification;
  private UUID resourceId;
  private UUID resourceGroupId;
  private ItemType resourceType;
  private PgPool pool;
  private User provider;
  private String resourceServerUrl;
  private AuthClient authClient;
  private String consumerRsUrl;
  private JsonObject additionalInfo = new JsonObject();

  public CreateNotification(
      PostgresService postgresService,
      CatalogueClient catalogueClient,
      EmailNotification emailNotification,
      AuthClient authClient) {
    this.postgresService = postgresService;
    this.catalogueClient = catalogueClient;
    this.emailNotification = emailNotification;
    this.authClient = authClient;
  }

  /**
   * Initiates the process of creating notifications by letting the request information go through
   * multiple checks
   *
   * @param notification request body for the POST Notification API with type JsonObject
   * @param user details of the consumer
   * @return response as JsonObject with type Future
   */
  public Future<JsonObject> initiateCreateNotification(JsonObject notification, User user) {
    resourceId = UUID.fromString(notification.getString("itemId"));
    String itemType = notification.getString("itemType");
    boolean isAdditionalInfoPresent = notification.containsKey("additionalInfo");
    if (isAdditionalInfoPresent) {
      boolean isAnyValueNull =
          notification.getJsonObject("additionalInfo").getMap().values().stream()
              .anyMatch(Objects::isNull);
      if (isAnyValueNull) {
        JsonObject failureMessage =
            new JsonObject()
                .put(TYPE, BAD_REQUEST.getValue())
                .put(TITLE, BAD_REQUEST_URN.getUrn())
                .put(DETAIL, FAILURE_MESSAGE + ", as additionalInfo contains a null value");
        return Future.failedFuture(failureMessage.encode());
      }
      additionalInfo = notification.getJsonObject("additionalInfo");
    }
    setConsumerRsUrl(user.getResourceServerUrl());

    /* check if the resource exists in CAT */
    Future<Boolean> getItemFromCatFuture = isItemPresentInCatalogue(resourceId, itemType);

    Future<Boolean> resourceInsertionFuture =
        getItemFromCatFuture.compose(
            resourceExistsInCatalogue -> {
              if (resourceExistsInCatalogue) {
                /* add the resource in resource_entity table if not already present*/
                return addResourceInDb(
                    INSERT_RESOURCE_INFO_QUERY,
                    resourceId,
                    getResourceGroupId(),
                    UUID.fromString(getProviderInfo().getUserId()),
                    getResourceServerUrl(),
                    getResourceType());
              }
              return Future.failedFuture(getItemFromCatFuture.cause().getMessage());
            });

    Future<Boolean> validPolicyExistsFuture =
        resourceInsertionFuture.compose(
            isResourceAddedInDb -> {
              if (isResourceAddedInDb) {
                return checkIfValidPolicyExists(GET_ACTIVE_CONSUMER_POLICY, resourceId, user);
              }
              /* something went wrong while inserting the resource in DB */
              return Future.failedFuture(resourceInsertionFuture.cause().getMessage());
            });

    Future<Boolean> validNotificationExistsFuture =
        validPolicyExistsFuture.compose(
            isValidPolicyExisting -> {
              /* Policy with ACTIVE status already present */
              if (isValidPolicyExisting) {
                return Future.failedFuture(validPolicyExistsFuture.cause().getMessage());
              }
              /* Policy doesn't exist, or is DELETED, or was expired */
              return checkIfValidNotificationExists(GET_VALID_NOTIFICATION, resourceId, user);
            });

    Future<JsonObject> createNotificationFuture =
        validNotificationExistsFuture.compose(
            isValidNotificationExisting -> {
              /* PENDING notification already exists waiting for its approval */
              if (isValidNotificationExisting) {
                return Future.failedFuture(validNotificationExistsFuture.cause().getMessage());
              }

              return createNotification(
                  CREATE_NOTIFICATION_WITH_ADDITIONAL_INFO_QUERY,
                  resourceId,
                  user,
                  UUID.fromString(getProviderInfo().getUserId()),
                  additionalInfo);
            });

    return createNotificationFuture;
  }

  /**
   * Adds resource in the database if the resource is not already present
   *
   * @param query An insert query
   * @param resourceId id of the resource with type UUID
   * @param resourceGroupId if present for the resource with type UUID or null
   * @param providerId id of the owner of the resource with type UUID
   * @param resourceServerUrl string containing resource-server-url of the item
   * @param itemType itemType of the item,can be either RESOURCE_GROUP or RESOURCE
   * @return True, if the insertion is successful or Failure if there is any DB failure
   */
  public Future<Boolean> addResourceInDb(
      String query,
      UUID resourceId,
      UUID resourceGroupId,
      UUID providerId,
      String resourceServerUrl,
      ItemType itemType) {
    Promise<Boolean> promise = Promise.promise();
    LOG.trace("inside addResourceInDb method");
    Tuple tuple = Tuple.of(resourceId, providerId, resourceGroupId, resourceServerUrl, itemType);
    executeQuery(
        query,
        tuple,
        handler -> {
          if (handler.succeeded()) {
            /* resource inserted successfully if not present */
            promise.complete(true);
          } else {
            promise.fail(handler.cause().getMessage());
          }
        });

    return promise.future();
  }

  /**
   * checks if the policy for the given resource and given consumer already exists or not <br>
   * If it is existing checks if it has been <b>DELETED</b> status or <b>EXPIRED</b> <br>
   * If the policy is in <b>ACTIVE</b> status then failure response is returned back
   *
   * @param query A SELECT query to fetch details about policy
   * @param resourceId id of the resource with type UUID
   * @param consumer Details of the user requesting to create notification with type User
   * @return False if policy is not present, <b>DELETED</b>, or <b>EXPIRED</b>. Failure if it is in
   *     <b>ACTIVE</b> status
   */
  public Future<Boolean> checkIfValidPolicyExists(String query, UUID resourceId, User consumer) {
    Promise<Boolean> promise = Promise.promise();
    LOG.trace("inside checkIfValidPolicyExists method");
    String consumerEmail = consumer.getEmailId();
    Tuple tuple = Tuple.of(consumerEmail, resourceId);
    executeQuery(
        query,
        tuple,
        handler -> {
          if (handler.succeeded()) {
            JsonArray result = handler.result().getJsonArray(RESULT);
            boolean isPolicyAbsent = result.isEmpty();
            if (isPolicyAbsent) {
              promise.complete(false);
            } else
            /* An active policy for the consumer is present */ {
              JsonObject failureMessage =
                  new JsonObject()
                      .put(TYPE, HttpStatusCode.CONFLICT.getValue())
                      .put(TITLE, POLICY_ALREADY_EXIST_URN.getUrn())
                      .put(DETAIL, FAILURE_MESSAGE + ", as a policy is already present");
              promise.fail(failureMessage.encode());
            }
          } else {
            promise.fail(handler.cause().getMessage());
          }
        });
    return promise.future();
  }

  /**
   * Verifies if the notification is already present by checking if it is <b>PENDING</b> status and
   * if the resource has <br>
   * been previously requested by the given user
   *
   * @param query A SELECT query to fetch details about the notification
   * @param resourceId id of the resource with type UUID
   * @param user consumer details with type User
   * @return False if the notification was not previously created, failure response if the
   *     notification was previously created
   */
  public Future<Boolean> checkIfValidNotificationExists(String query, UUID resourceId, User user) {
    Promise<Boolean> promise = Promise.promise();
    LOG.trace("inside checkIfValidNotificationExists method");
    UUID consumerId = UUID.fromString(user.getUserId());
    Tuple tuple = Tuple.of(consumerId, resourceId);
    executeQuery(
        query,
        tuple,
        handler -> {
          if (handler.succeeded()) {
            JsonArray result = handler.result().getJsonArray(RESULT);
            boolean isNotificationAbsent = result.isEmpty();
            if (isNotificationAbsent) {
              promise.complete(false);
            } else {
              /* A notification was created previously by the consumer and is in PENDING status */
              JsonObject failureResponse =
                  new JsonObject()
                      .put(TYPE, HttpStatusCode.CONFLICT.getValue())
                      .put(TITLE, POLICY_ALREADY_EXIST_URN.getUrn())
                      .put(
                          DETAIL,
                          FAILURE_MESSAGE
                              + ", as a request for the given resource has been previously made");
              promise.fail(failureResponse.encode());
            }
          } else {
            promise.fail(handler.cause().getMessage());
          }
        });
    return promise.future();
  }

  /**
   * Creates notification for the consumer to access the given resource
   *
   * @param query Insert query to create notification
   * @param resourceId id for which the consumer or consumer delegate wants access to with type UUID
   * @param consumer details of the consumer with type User
   * @param providerId id of the owner of the resource with type UUID
   * @param additionalInfo details of the data consumer if resource is being used for non-commercial
   *     purposes
   * @return JsonObject response, if notification is created successfully, failure if any
   */
  public Future<JsonObject> createNotification(
      String query, UUID resourceId, User consumer, UUID providerId, JsonObject additionalInfo) {
    Promise<JsonObject> promise = Promise.promise();
    LOG.trace("inside createNotification method");
    UUID consumerId = UUID.fromString(consumer.getUserId());
    Tuple tuple = Tuple.of(consumerId, resourceId, providerId, additionalInfo);

    executeQuery(
        query,
        tuple,
        handler -> {
          if (handler.succeeded()) {
            JsonArray result = handler.result().getJsonArray(RESULT);
            if (result.isEmpty()) {
              /*notification id not returned*/
              JsonObject failureMessage =
                  new JsonObject()
                      .put(TYPE, INTERNAL_SERVER_ERROR.getValue())
                      .put(TITLE, ResponseUrn.INTERNAL_SERVER_ERROR.getUrn())
                      .put(DETAIL, FAILURE_MESSAGE);
              promise.fail(failureMessage.encode());
            } else {
              LOG.info(
                  "created a notification with notification Id : {}",
                  result.getJsonObject(0).getString("_id"));
              JsonObject response =
                  new JsonObject()
                      .put(TYPE, ResponseUrn.SUCCESS_URN.getUrn())
                      .put(TITLE, ResponseUrn.SUCCESS_URN.getMessage())
                      .put(DETAIL, "Request inserted successfully!")
                      .put(STATUS_CODE, HttpStatusCode.SUCCESS.getValue());

              /* send email to the provider saying this consumer has requested for the access of this resource */
              emailNotification.sendEmail(
                  consumer, this.getProviderInfo(), resourceId.toString(), getResourceServerUrl());

              promise.complete(response);
            }
          } else {
            promise.fail(handler.cause().getMessage());
          }
        });
    return promise.future();
  }

  /**
   * Checks if the item given in the request is present in Catalogue. It will get the information
   * related to resource like <br>
   * resourceGroupId, providerId and stores these values to be used further
   *
   * @param resourceId or itemId of the given resource with type UUID
   * @param itemType type of the resource present in the request body
   * @return True, if information is fetched successfully, failure if there is no resource in the
   *     CAT with the given id or if any other failure occurs
   */
  public Future<Boolean> isItemPresentInCatalogue(UUID resourceId, String itemType) {
    Promise<Boolean> promise = Promise.promise();
    LOG.trace("inside isItemPresentInCatalogue method");
    Set<UUID> uuidSet = Set.of(resourceId);
    catalogueClient
        .fetchItems(uuidSet)
        .onComplete(
            handler -> {
              if (handler.succeeded()) {
                ResourceObj result = handler.result().get(0);
                final UUID ownerId = result.getProviderId();
                UUID resourceGroupIdValue = result.getResourceGroupId();
                String url = result.getResourceServerUrl();

                boolean isItemTypeMatching = itemType.equals(result.getItemType().toString());
                if (isItemTypeMatching) {
                  setResourceType(result.getItemType());
                  /* set provider id, resourceGroupId, resourceType, resource server url */
                  setResourceServerUrl(url);
                  /* set provider id, resourceGroupId, resourceType */
                  setResourceGroupId(resourceGroupIdValue);

                  /* get information about the provider of the resource from Auth*/
                  UserInfo provider =
                      new UserInfo()
                          .setUserId(ownerId)
                          .setRole(DxRole.PROVIDER)
                          .setAudience(url)
                          .setDelegate(false);

                  /* check if the resource server url of the user matches with the resource */
                  boolean isConsumerBelongingToSameServerAsItem = url.equals(getConsumerRsUrl());
                  if (isConsumerBelongingToSameServerAsItem) {
                    authClient
                        .fetchUserInfo(provider)
                        .onComplete(
                            authHandler -> {
                              if (authHandler.succeeded()) {
                                User providerInfo = authHandler.result();
                                setProviderInfo(providerInfo);
                                promise.complete(true);
                              } else {
                                LOG.debug(
                                    "Something went wrong while fetching provider information from Auth {}",
                                    authHandler.cause().getMessage());
                                JsonObject failureMessage =
                                    new JsonObject()
                                        .put(TYPE, INTERNAL_SERVER_ERROR.getValue())
                                        .put(TITLE, ResponseUrn.INTERNAL_SERVER_ERROR.getUrn())
                                        .put(DETAIL, FAILURE_MESSAGE);
                                promise.fail(failureMessage.encode());
                              }
                            });
                  } else {
                    LOG.debug(
                        "user does not have access to create notification as they're belonging "
                            + "to a different server w.r.t to the resource");
                    JsonObject failureMessage =
                        new JsonObject()
                            .put(TYPE, HttpStatusCode.NOT_FOUND.getValue())
                            .put(TITLE, ResponseUrn.RESOURCE_NOT_FOUND_URN.getUrn())
                            .put(
                                DETAIL,
                                "Access request could not be created, as resource was not found");
                    promise.fail(failureMessage.encode());
                  }
                } else {
                  /*item type in the request body does not match the item type from the catalogue*/
                  LOG.debug(
                      "Item type in the request body is {} and item type from catalogue is {}",
                      itemType,
                      result.getItemType().toString());
                  JsonObject failureMessage =
                      new JsonObject()
                          .put(TYPE, BAD_REQUEST.getValue())
                          .put(TITLE, ResponseUrn.BAD_REQUEST_URN.getUrn())
                          .put(DETAIL, FAILURE_MESSAGE + ", as the item type is invalid");
                  promise.fail(failureMessage.encode());
                }
              } else {
                if (handler.cause().getMessage().equalsIgnoreCase("Item is not found")
                    || handler
                        .cause()
                        .getMessage()
                        .equalsIgnoreCase("Id/Ids does not present in CAT")
                    || handler
                        .cause()
                        .getMessage()
                        .equalsIgnoreCase("Item id given is not present")) {
                  /*id not present in the catalogue*/
                  JsonObject failureMessage =
                      new JsonObject()
                          .put(TYPE, HttpStatusCode.NOT_FOUND.getValue())
                          .put(TITLE, ResponseUrn.RESOURCE_NOT_FOUND_URN.getUrn())
                          .put(DETAIL, FAILURE_MESSAGE + ", as resource was not found");
                  promise.fail(failureMessage.encode());
                } else if (handler.cause().getMessage().contains("Given id is invalid")
                    || handler.cause().getMessage().contains("Resource is forbidden to access")) {
                  promise.fail(handler.cause().getMessage());
                } else {
                  /*something went wrong while fetching the item from catalogue*/
                  LOG.error(
                      "Failure while fetching item from CAT : {}", handler.cause().getMessage());
                  JsonObject failureMessage =
                      new JsonObject()
                          .put(TYPE, INTERNAL_SERVER_ERROR.getValue())
                          .put(TITLE, ResponseUrn.INTERNAL_SERVER_ERROR.getUrn())
                          .put(DETAIL, FAILURE_MESSAGE);
                  promise.fail(failureMessage.encode());
                }
              }
            });
    return promise.future();
  }

  /**
   * Executes the query by getting the Pgpool instance from postgres
   *
   * @param query to be executes
   * @param tuple exchangeable values to be added in the query
   * @param handler AsyncResult JsonObject handler
   */
  public void executeQuery(String query, Tuple tuple, Handler<AsyncResult<JsonObject>> handler) {
    LOG.trace("inside executeQuery method");
    pool = postgresService.getPool();
    Collector<Row, ?, List<JsonObject>> rowListCollector =
        Collectors.mapping(row -> row.toJson(), Collectors.toList());

    pool.withConnection(
            sqlConnection ->
                sqlConnection
                    .preparedQuery(query)
                    .collecting(rowListCollector)
                    .execute(tuple)
                    .map(rows -> rows.value()))
        .onSuccess(
            successHandler -> {
              JsonArray response = new JsonArray(successHandler);
              JsonObject responseJson =
                  new JsonObject()
                      .put(TYPE, ResponseUrn.SUCCESS_URN.getUrn())
                      .put(TITLE, ResponseUrn.SUCCESS_URN.getMessage())
                      .put(RESULT, response);
              handler.handle(Future.succeededFuture(responseJson));
            })
        .onFailure(
            failureHandler -> {
              LOG.error("Failure while executing the query : {}", failureHandler.getMessage());
              JsonObject response =
                  new JsonObject()
                      .put(TYPE, INTERNAL_SERVER_ERROR.getValue())
                      .put(TITLE, ResponseUrn.DB_ERROR_URN.getUrn())
                      .put(DETAIL, "Failure while executing query");
              handler.handle(Future.failedFuture(response.encode()));
            });
  }

  public User getProviderInfo() {
    return this.provider;
  }

  public void setProviderInfo(User user) {
    provider = user;
  }

  public UUID getResourceGroupId() {
    return resourceGroupId;
  }

  public void setResourceGroupId(UUID resourceGroupId) {
    this.resourceGroupId = resourceGroupId;
  }

  public String getResourceServerUrl() {
    return this.resourceServerUrl;
  }

  public void setResourceServerUrl(String resourceServerUrl) {
    this.resourceServerUrl = resourceServerUrl;
  }

  public ItemType getResourceType() {
    return this.resourceType;
  }

  public void setResourceType(ItemType resourceType) {
    this.resourceType = resourceType;
  }

  public String getConsumerRsUrl() {
    return consumerRsUrl;
  }

  public void setConsumerRsUrl(String consumerRsUrl) {
    this.consumerRsUrl = consumerRsUrl;
  }
}
