|||
|:--|:--|
|**Status**| In-Progress|
|**Prepared-By**| VS, MM|
|**Prepared-For**| ODA developers |

# Purpose

To describe flow of the tokens in ODA.

# User tokens

## Generation

We use [JWT](https://jwt.io/introduction/) to authentify users.

We create JWT in [drupal](https://github.com/oda-hub/frontend-chart), using symmetric encryption (currently), secret injected [at the deployment](https://github.com/oda-hub/frontend-chart/issues/7)

JWT will be provided by frontend in each request, once the user is logged in. In particular:
* the cookie Drupal.visitor.token is defined and it contains a JWT
* the token is also provided as a paramter, <em>token</em>, within the URL for each request to the dispatcher
* the token is also sent in the request header authorization

In the event of a public request, so one executed with no user logged in, the token won't be provided.

JWT [will be also](https://github.com/oda-hub/frontend-astrooda/issues/1) made available to the user for use in the API

The payload part of the token contains the user email, name, roles and the expiration time of the token itself.

Example:
```json
{
  "email": "mtm@mtmco.net",
  "name": "mmeharga",
  "roles": "authenticated user, content manager, general, magic",
  "exp": 1613662947
}
```

The <em>exp</em> (expiration time) field identifies the expiration time on or after which the JWT MUST NOT be accepted for processing,
and it is defined as a timestamp in seconds since the epoch (as specified [here](https://tools.ietf.org/html/rfc7519#section-2)).
This can be defined in the Drupal administration GUI.

## Usage

Token can be used by the frontend to hide some parts of the interface, 
but they will be mainly used by the **dispatcher** as well as some **backends** to allocate resources.

See current working definition of real roles [here](https://github.com/oda-hub/doc-multi-user/blob/main/plan-roles-users.md).

* **Roles** will be used by the **dispatcher** when filtering queries. The **Dispatcher** may associate each query with the required **Roles**, or establish filters for more complex request matching (e.g. restricting parameter ranges). We should be cautious to put too much configuration in dispatcher when other methods (below) are possible.

In general, backends should be able to control access to themselves by instructing dispatcher, see [interface](plugin-interface.md) for dispatcher to learn about role restrictions from backend:

* Some **Backends** may **declare which roles they require**, and **dispatcher** will respect these requests. See [interface](plugin-interface.md) for dispatcher to learn about role restrictions from backend.
* Some **Backends** (e.g. integral) will declare that they can operate in role-restricted mode, and will themselves perform additional filtering based on the **Role** information provided.

In the second case, a more restricted **Token** can be provided to the backend, to prevent backend gaining entire rights of the user. **Restricted Token** can be obtained (on a special service, or another authority source) in exchange for the User **Token**, specifying the narrowing down of the scope.

## TODO: rescued definition part of the previously doc, to adapt

users will be identified by JWT user **Token**. **Token** will container user identity (with email)
**Token** will be signed (HMAC) with shared secret or (maybe better but a bit more elaborate) asymmetric encryption.

**Token** will contain usual fields for validity time and scope of application.

Example token (following the [standard](https://tools.ietf.org/html/rfc7519#section-4))

```json
{
 "sub": "user-name@host.name",
}
```

Passing the token:

* request header with [Bearer](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Authorization)
* *(optional)* request header with cookie, _oauth2_token

**Token** will be complemneted by information about **Role** binding, schematically:

```
roleBinding:
  subject: User 
  role: Role
```

User may have many roles.
**Role** information can be retrieved at any time from the **Registry** (probably at frontend level, but could be combined between frontend and additional dispatcher-level service, if frontend will not provide sufficient detail).

**Roles** will correspond to particular actions on particular resources. 

Example roles:

* *unige-hpc-full* - will be granted full access to HPC clusters
* *public-pool-hpc* 
* *integral-private-qla* - access to private data for QLA purposes
* *magic* - access magic backend

See current working definition of real roles [here](https://github.com/oda-hub/doc-multi-user/blob/main/plan-roles-users.md).

**Roles** will be used by **dispatcher** when filtering queries. **Dispatcher** may associate each query with required **Roles**, or establish filters for more complex request matching (e.g. restricting parameter ranges).

Some **Backends** may declare which roles they require, and **dispatcher** will respect these requests. 

Some **backends** (e.g. integral) will declare that they can operate in role-restricted mode, and will perform additional filtering based on the **Role** information provided.

In this case, a more restricted **Token** can be provided to the backend, to prevent backend gaining entire rights of the user. **Restricted Token** can be obtained (on a special service, or another authority source) in exchange for the User **Token**, specifying the narrowing down of the scope.


## Actual roles

See current working definition of real roles [here](https://github.com/oda-hub/doc-multi-user/blob/main/plan-roles-users.md).


## Other Applicable Documents

|||
| :-- | :-- |
| Meetings | [2021-02-22](https://github.com/oda-hub/meetings/blob/main/2021-02-22/MoM.md) |


## Other sources  of identity

source of identity:

    drupal user account can be used
    github/gmail
    unige ldap
    unige ISIS?
    switch?
    edugain?

