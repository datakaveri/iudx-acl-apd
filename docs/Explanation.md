<p align="center">
<img src="./cdpg.png" width="300">
</p>

# Tokens, Users and Roles
## Tokens used in DX
- Tokens for a user could be created using DX AAA Server API : [link to the API docs](https://authorization.iudx.org.in/apis#tag/Token-APIs/operation/post-auth-v1-token). The token used in DX are :

| Token             |                                                                                     Purpose                                                                                     | Users                                                                      |
|:------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:---------------------------------------------------------------------------|
| DX Identity token |                                          Serves as an identifier of the user to the server to access the ACL-APD Server's capabilities                                          | Provider, provider delegate, consumer, consumer delegate, cos admin, admin |
| Keycloak token    | Access token as bearer credential is generated by Keycloak by providing the client's email ID and password and then adding bearer to the access token : `bearer <access-token>` | DX AAA Server, Users                                                       |
| Access token      |                                                                    To get access to resource, resource group                                                                    | Provider, provider delegate, consumer, consumer delegate                   |

## Tokens accepted in DX ACL APD Server
Tokens accepted by the DX ACL APD Server are the DX Identity token and keycloak token. Identity token is used for user specific APIs whereas the bearer token is used for the Verify API.
Providers, Consumers and delegates of providers and consumers are allowed to access the following APIs using the mentioned token:

| API                     |                          Users                           | Token             |
|:------------------------|:--------------------------------------------------------:|:------------------|
| Create Policy           |               Provider, provider delegate                | DX Identity token |
| Update access request   |               Provider, provider delegate                | DX Identity token |
| Delete Policy           |               Provider, provider delegate                | DX Identity token |
| Create Access Request   |               Consumer, consumer delegate                | DX Identity token |
| Withdraw access request |               Consumer, consumer delegate                | DX Identity token |
| Get Policies            | Consumer, provider, consumer delegate, provider delegate | DX Identity token |
| Get Access Requests     | Consumer, provider, consumer delegate, provider delegate | DX Identity token |
| Verify Policy           |                      DX AAA Server                       | Keycloak token    |

## Users and Roles
All registered users of DX can access the DX ACL APD Server. The DX ACL APD Server identifies the user based on the token information which is provided by DX AAA Server. 

<div style="text-align: center;">
<img src="./users-and-roles.png" alt="Users and Roles" width="600" height="400"/>
</div>

How is the user considered as a consumer, provider or delegate?
- While decoding the token at the DX ACL APD Server, the **role** in token fetched from DX AAA Server and then the following rules is applied to identify the user
  - A user is considered as a **provider** if **role** is **provider** 
  - A user is considered as a **consumer** if **role** is **consumer** 
  - A user is considered as a delegate of the consumer if **role** is **delegate** and **drl** is **consumer**
  - A user is considered as a delegate of the provider if **role** is **delegate** and **drl** is **provider**

## Terminologies and Definitions
- **Policy** : An agreement or contract between the owner of the resource to allow the consumer to access the resource
- **Notification** : (or access request) Is a requisition from consumer to owner of the resource to access the resource
- **Constraints** : (or capabilities) Are different methods in which information related to resource can be fetched
- **Policy Status** : Policy could be in either of these states - active, deleted or expired
- **Notification Status** : Notification could be in either of these states - pending, granted, rejected, withdrawn
- **Delegate** : Consumer or provider appointed user who could act on behalf of the delegator

