package iudx.apd.acl.server.metering;

import static iudx.apd.acl.server.apiserver.util.Constants.BODY;
import static iudx.apd.acl.server.apiserver.util.Constants.EPOCH_TIME;
import static iudx.apd.acl.server.apiserver.util.Constants.ISO_TIME;
import static iudx.apd.acl.server.apiserver.util.Constants.USER_ID;
import static iudx.apd.acl.server.metering.util.Constant.METERING_SERVICE_ADDRESS;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;
import io.vertx.serviceproxy.ServiceBinder;
import iudx.apd.acl.server.metering.databroker.DataBrokerService;
import iudx.apd.acl.server.metering.databroker.DataBrokerServiceImpl;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MeteringVerticle extends AbstractVerticle {
  private static final Logger LOGGER = LogManager.getLogger(MeteringVerticle.class);

  private RabbitMQOptions config;
  private RabbitMQClient client;
  private String dataBrokerIp;
  private int dataBrokerPort;
  private int dataBrokerManagementPort;
  private String dataBrokerVhost;
  private String dataBrokerUserName;
  private String dataBrokerPassword;
  private int connectionTimeout;
  private int requestedHeartbeat;
  private int handshakeTimeout;
  private int requestedChannelMax;
  private int networkRecoveryInterval;
  private ServiceBinder binder;
  private MessageConsumer<JsonObject> consumer;
  private WebClientOptions webConfig;
  private DataBrokerService dataBrokerService;
  private MeteringService metering;

  @Override
  public void start() throws Exception {

    /* Read the configuration and set the rabbitMQ server properties. */
    dataBrokerIp = config().getString("dataBrokerIP");
    dataBrokerPort = config().getInteger("dataBrokerPort");
    dataBrokerManagementPort = config().getInteger("dataBrokerManagementPort");
    dataBrokerVhost = config().getString("dataBrokerVhost");
    dataBrokerUserName = config().getString("dataBrokerUserName");
    dataBrokerPassword = config().getString("dataBrokerPassword");
    connectionTimeout = config().getInteger("connectionTimeout");
    requestedHeartbeat = config().getInteger("requestedHeartbeat");
    handshakeTimeout = config().getInteger("handshakeTimeout");
    requestedChannelMax = config().getInteger("requestedChannelMax");
    networkRecoveryInterval = config().getInteger("networkRecoveryInterval");

    /* Configure the RabbitMQ Data Broker client with input from config files. */

    config = new RabbitMQOptions().setUser(dataBrokerUserName).setPassword(dataBrokerPassword)
        .setHost(dataBrokerIp).setPort(dataBrokerPort).setVirtualHost(dataBrokerVhost)
        .setConnectionTimeout(connectionTimeout).setRequestedHeartbeat(requestedHeartbeat)
        .setHandshakeTimeout(handshakeTimeout).setRequestedChannelMax(requestedChannelMax)
        .setNetworkRecoveryInterval(networkRecoveryInterval).setAutomaticRecoveryEnabled(true);

    webConfig = new WebClientOptions().setKeepAlive(true).setConnectTimeout(86400000)
        .setDefaultHost(dataBrokerIp).setDefaultPort(dataBrokerManagementPort)
        .setKeepAliveTimeout(86400000);
    /*
     * Create a RabbitMQ Client with the configuration and vertx cluster instance.
     */

    client = RabbitMQClient.create(vertx, config);
    dataBrokerService = new DataBrokerServiceImpl(client);
    metering = new MeteringServiceImpl(dataBrokerService);
    binder = new ServiceBinder(vertx);
    /* Publish the Data Broker service with the Event Bus against an address. */
    consumer =
        binder.setAddress(METERING_SERVICE_ADDRESS).register(MeteringService.class, metering);
  }
  @Override
  public void stop() {
    binder.unregister(consumer);
  }

}