![IUDX](./iudx.png)

# Setup and Installation Guide
This document contains the installation and configuration information required to deploy the Data Exchange (DX) ACL APD Server.

## Configuration
In order to connect the DX ACL APD Server with PostgreSQL, RabbitMQ, Email Service, DX Catalogue Server, DX AAA Server, etc please refer [Configurations](https://github.com/datakaveri/iudx-acl-apd/blob/main/docs/Configurations.md). It contains appropriate information which shall be updated as per the deployment. 

## Dependencies
In this section we explain about the dependencies and their scope.  It is expected that the dependencies are met before starting the deployment of DX ACL APD Server.
### External Dependencies
| Software Name    | Purpose                                                                                                                          | 
|:-----------------|:---------------------------------------------------------------------------------------------------------------------------------|
| PostgreSQL       | For storing information related to policy, access Request based CRUD operations, approved access requests, resources and users   |
| RabbitMQ         | To publish auditing related data to auditing server via RMQ exchange                                                             |
| SMTP Mail Server | To send email notifications to provider, provider delegates when access requests are created by the consumer, consumer delegates |


### Internal Dependencies
| Software Name                                              | Purpose                                                               | 
|:-----------------------------------------------------------|:----------------------------------------------------------------------|
| DX Authentication Authorization and Accounting (AAA) Server   | Used to download certificate for JWT token decoding and to get user info |
| DX Catalogue Server                                           | Used to fetch the list of resource and provider related information   |

### Prerequisites
### Keycloak registration for DX ACL-APD as trustee and APD
- The trustee user must be registered on Keycloak as a user
    - This can be done via the keycloak admin panel, or by using Data Exchange (DX) UI
    - The trustee user need not have any roles beforehand
- The COS admin user must call the create APD API : [Link to the API](https://authorization.iudx.org.in/apis#tag/Access-Policy-Domain-(APD)-APIs/operation/post-auth-v1-apd)
  with the name as the name of the APD, owner as email address of trustee (same as whatever is registered on Keycloak)
  and URL as the domain of the APD
- Once the APD has been successfully registered, the trustee user will gain the trustee role
  scoped to that particular APD.
    - They can verify this by calling the list user roles API : [Link to the API](https://authorization.iudx.org.in/apis#tag/User-APIs/operation/get-auth-v1-user-roles)
- The trustee can get client credentials to be used in APD Operations by calling the
  get default client credentials API : [Link to the API](https://authorization.iudx.org.in/apis#tag/User-APIs/operation/get-auth-v1-user-clientcredentials)

#### RabbitMQ
- To setup RMQ refer the docker files available [here](https://github.com/datakaveri/iudx-deployment/blob/master/Docker-Swarm-deployment/single-node/databroker)

##### vHost table
| vHost         |  
|---------------|
| IUDX-INTERNAL |

##### Exchange table

| Exchange Name | Type of exchange | features |   
|---------------|------------------|----------|
| auditing      | direct           | durable  | 


##### Queue table


| Exchange Name | Queue Name | vHost   | routing key |
|---------------|------------|---------|-------------|
| auditing      | direct     | durable | #           |

##### Permissions
ACL-APD user could have write permission as it publishes audit data
```
 "permissions": [
        {
          "vhost": "IUDX-INTERNAL",
          "permission": {
            "configure": "^$",
            "write": "^auditing$",
            "read": "^$"
          }
        }
]
```


#### PostgresQL
- To setup PostgreSQL refer the docker files available [here](https://github.com/datakaveri/iudx-deployment/blob/master/Docker-Swarm-deployment/single-node/postgres)
- **Note** : PostgreSQL database should be configured with a RBAC user having CRUD privileges

| Table Name               | Purpose                                                                                                                                                                  | 
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| user_table               | To store user related information like first name, last name, email etc, that is fetched from AAA Server                                                                 |
| resource_entity          | To store resource information fetched from catalogue                                                                                                                     | 
| policy                   | To store policy related information, resource info, consumer and provider info                                                                                           | 
| request                  | To store the access request related information whenever an access request is created by a consumer to request provider to create policy for a resource / resource group |
| approved_access_requests | To store approved notifications when the provider sets the notification status to granted inorder to create policy                                                       |

#### Auditing
- Auditing is done using Immudb and Postgres DB
- To Setup immuclient for immudb please refer [immudb setup guide](https://github.com/datakaveri/iudx-deployment/tree/master/docs/immudb-setup)
- Schema for Auditing table in PostgreSQL is present here - [postgres auditing table schema](https://github.com/datakaveri/iudx-resource-server/blob/master/src/main/resources/db/migration/V5_2__create-auditing-acl-apd-table.sql)
- Schema for Immudb table, index for the table is present here - [immudb schema in DX Auditing Server](https://github.com/datakaveri/auditing-server/tree/main/src/main/resources/immudb/migration)

| Table Name               | Purpose                                                                                                                                             | DB                 | 
|--------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|
| auditing_acl_apd         | To store logged information about endpoint, caller of the endpoint, timestamp when the POST, DELETE, PUT requests respond with 200 success response | Immudb, PostgreSQL |

- User ID, endpoint and epoch time are indexed in Immudb to retrieve the logs faster  

#### Flyway Migrations
- Database flyway migrations help in updating the schema, permissions, grants, triggers etc., with the latest version
- Each flyway schema file is versioned with the format `V<number>_<number>__file-name.sql`, ex : `V1_1__init-tables.sql`
- Schemas for PostgreSQL tables are present here - [Flyway schema](https://github.com/datakaveri/iudx-acl-apd/tree/main/src/main/resources/db/migration)
- Values like DB URL, database user credentials, user and schema name could be populated in flyway.conf
- The following commands could be executed
  - ``` mvn flyway:info -Dflyway.configFiles=flyway.conf``` To get the flyway schema history table
  - ``` mvn flyway:migrate -Dflyway.configFiles=flyway.conf ``` To migrate flyway schema 
  - ``` mvn flyway:repair ``` To resolve some migration errors during flyway migration
- Please find the reference to Flyway migration [here](https://documentation.red-gate.com/fd/migrations-184127470.html)

## Installation Steps
### Maven
1. Install java 11 and maven
2. Use the maven exec plugin based starter to start the server
   `mvn clean compile exec:java@acl-apd-server`

### JAR
1. Install java 11 and maven
2. Set Environment variables
```
export ACL_APD_URL=https://<acl-apd-domain-name>
export LOG_LEVEL=INFO
```
3. Use maven to package the application as a JAR
   `mvn clean package -Dmaven.test.skip=true`
4. 2 JAR files would be generated in the `target/` directory
    - `iudx.iudx.apd.acl.server-cluster-0.0.1-SNAPSHOT-fat.jar` - clustered vert.x containing micrometer metrics
    - `iudx.iudx.apd.acl.server-dev-0.0.1-SNAPSHOT-fat.jar` - non-clustered vert.x and does not contain micrometer metrics

#### Running the clustered JAR
**Note**: The clustered JAR requires Zookeeper to be installed. Refer [here](https://zookeeper.apache.org/doc/r3.3.3/zookeeperStarted.html) to learn more about how to set up Zookeeper. Additionally, the `zookeepers` key in the config being used needs to be updated with the IP address/domain of the system running Zookeeper.
The JAR requires 3 runtime arguments when running:

* --config/-c : path to the config file
* --hostname/-i : the hostname for clustering
* --modules/-m : comma separated list of module names to deploy

e.g. ```java -jar target/iudx.iudx.apd.acl.server-cluster-0.0.1-SNAPSHOT-fat.jar --host $(hostname)
-c secrets/all-verticles-configs/config-dev.json -m iudx.apd.acl.server.authentication.AuthenticationVerticle, iudx.apd.acl.server.apiserver.ApiServerVerticle,
iudx.apd.acl.server.policy.PolicyVerticle, iudx.apd.acl.server.notification.NotificationVerticle, iudx.apd.acl.server.auditing.AuditingVerticle```

Use the `--help/-h` argument for more information. You may additionally append an `ACL_APD_JAVA_OPTS` environment
variable containing any Java options to pass to the application.

e.g.
```
$ export ACL_APD_JAVA_OPTS="Xmx40496m"
$ java $ACL_APD_JAVA_OPTS -jar target/iudx.iudx.apd.acl.server-cluster-0.0.1-SNAPSHOT-fat.jar ...

```


#### Running the non-clustered JAR
The JAR requires 1 runtime argument when running

* --config/-c : path to the config file

e.g. `java -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.Log4j2LogDelegateFactory -jar target/iudx.iudx.apd.acl.server-cluster-0.0.1-SNAPSHOT-fat.jar -c secrets/all-verticles-configs/config-dev.json`

Use the `--help/-h` argument for more information. You may additionally append an `RS_JAVA_OPTS` environment variable containing any Java options to pass to the application.

e.g.
```
$ export ACL_APD_JAVA_OPTS="-Xmx1024m"
$ java ACL_APD_JAVA_OPTS -jar target/iudx.iudx.apd.acl.server-cluster-0.0.1-SNAPSHOT-fat.jar ...
```

### Docker
1. Install docker and docker-compose
2. Clone this repo
3. Build the images
   ` ./docker/build.sh`
4. Modify the `docker-compose.yml` file to map the config file you just created
5. Start the server in production (prod) or development (dev) mode using docker-compose
   ` docker-compose up prod `
### Maven based
1. Install java 11 and maven
2. Use the maven exec plugin based starter to start the server
   `mvn clean compile exec:java@acl-apd-server`

## Logging and Monitoring
### Log4j 2
- For asynchronous logging, logging messages to the console in a specific format, Apache log4j 2 is used
- For log formatting, adding appenders, adding custom logs, setting log levels, log4j2.xml could be updated : [link](https://github.com/datakaveri/iudx-acl-apd/blob/main/src/main/resources/log4j2.xml) 
- Please find the reference to [link](https://logging.apache.org/log4j/2.x/manual/index.html)

### Micrometer
- #TODO

## Testing
### Unit Testing
1. Run the server through either docker, maven or redeployer
2. Run the unit tests and generate a surefire report
   `mvn clean test-compile surefire:test surefire-report:report`
3. Jacoco reports are stored in `./target/`
4. A sample recording to execute unit test is available [here](https://github.com/datakaveri/iudx-acl-apd/blob/main/unitTest.cast) 



### Integration Testing

Integration tests are through Postman/Newman whose script can be found from [here](https://github.com/datakaveri/iudx-acl-apd/tree/main/src/test/resources).
1. Install prerequisites
- [postman](https://www.postman.com/) + [newman](https://www.npmjs.com/package/newman)
- [newman reporter-htmlextra](https://www.npmjs.com/package/newman-reporter-htmlextra)
2. Example Postman environment can be found [here](https://github.com/datakaveri/iudx-acl-apd/blob/main/src/test/resources/IUDX-ACL-APD-APIs.postman_environment.json)
- Please find the README to setup postman environment file [here](https://github.com/datakaveri/iudx-acl-apd/blob/main/src/test/resources/README.md)
3. Run the server through either docker, maven or redeployer
4. Run the integration tests and generate the newman report
   `newman run <postman-collection-path> -e <postman-environment> --insecure -r htmlextra --reporter-htmlextra-export .`
5. Reports are stored in `./target/`


### Performance Testing
- JMeter
Explain how to execute tests. #TODO

### Security Testing
- For security testing, Zed Attack Proxy(ZAP) Scanning is done to discover security risks, vulnerabilities to help us address them
- A report is generated to show vulnerabilities as high risk, medium risk, low risk and false positive 
- Please find the reference to ZAP : [here](https://www.zaproxy.org/getting-started/)
