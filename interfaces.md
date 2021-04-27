|||
|:--|:--|
|**Status**| In-Progress|
|**Prepared-By**| VS, MM, CF|
|**Prepared-For**| ODA developers |

# Purpose

To describe flow of the tokens in ODA and reference to roles and user definition

# General approach to treating users

Users may fall in one of the two classes:

| | |
| :-- | :-- |
| anonymous | User that decides not to register to the website, we will try to discourage, but not ban. Will have access to the k8s cluster resources, no access to  UNIGE HPC clusters or any kind of private data, described below. |
| authenticated | the same exact rights as those of **anonymous**, but will receive e-mail notification when job is submitted and completed, can be tracked by developers for requests and errors following specific consent. |

Note that priviledges are only set by roles, hence user types separation is orthogonal to role selection. I.e. in principle, any user type can have any role combination.
Without explicit specification of roles, both **authenticated** and **anonymous** user have the same priveledges if roles are not set.
However, since roles are set with user attributes in the user **token**, generally only **authenticated** users can have additional roles.
Note that it is possible (and indeed necessary) to sometimes create different tokens with the same user id (email) but different role selection. It is not necessary to address this complexity with drupal.

Meaning of Roles and Source of Roles definition are described below.

# Roles

Roles are defined as simple strings, case-insensitive, matching "`[a-z][0-9]\-`". They may be also referenced as URI, http://odahub.io/roles/XXXXX

## Origin of Roles

The belonging to groups in the Drupal can be used adapted to reflect roles.
In Drupal, groups characterize users inside the CMS. Besides **anonymous** (not logged and identified) and **authenticated** (identified by the compulsory user attributes), we define a collection of necessary groups. Note **anonymous** and **authenticated** are not roles since they do not provide any priviledge by default.

Roles can be also defined by other means, i.e in dispatcher. For most purposes, ultimate **origin** of  roles is defined by the consumer of the role they are used: dispatcher plugins and backends (see below).

## Meaning of Roles

Note that roles define ability to act in certain way. Assigning a role means giving ability to act, while the basic usage of the infrastructure is exploited without a specific role. Roles should be designed not to overlap or inherit from each other. 

* Roles assigned to user are strictly **additive**: user which has a collection of roles, has added priviledges given by all of them.
* Roles as used at validation  are  **multiplicative**: i.e. requesing a resource demanding a collection of roles needs each of them to be satistifed. Treatment of the roles is also often more complex, and uses a combination of request parameters with roles, as implemented by dispatcher and instrument plugins.

## Usage of Roles

### In Frontend

Roles can be used by the frontend to hide some parts of the interface.

### In Dispatcher

**Roles** will be used by the **dispatcher** when filtering queries. The **Dispatcher** may associate each query with the required **Roles**, or establish filters for more complex request matching (e.g. restricting parameter ranges). We should be cautious to put too much configuration in dispatcher when other methods (below) are possible.


In general, the authority to demand roles should be given to plugins when possible.
They should be able to control access to themselves by instructing the dispatcher, with default plugin roles set in **plugin configuriation**. 

Dispatcher (and instrument plugin) treatment of the may be more complex, and uses combination of request parameters with roles by overriding dispatcher [method](https://github.com/oda-hub/dispatcher-app/blob/master/cdci_data_analysis/flask_app/dispatcher_query.py#L794).


### In Backends

Backends should be able to control access to themselves by instructing dispatcher. Dispatcher need not to bother about this particular case, it is treated in plugins end exposed through the same interface.

* Some **Backends** may **declare which roles they require**, and **dispatcher plugins** will respect these requests. General dispatcher needs not to be concerned with this. 
* Some **Backends** (e.g. integral) will declare that they can operate in role-restricted mode, and will themselves perform additional filtering based on the **Role** information provided.

In the second case, a more restricted token may be provided to the backend, to prevent backend gaining entire rights of the user. 

# User tokens

Roles are passed in user tokens.

We use [JWT](https://jwt.io/introduction/) ([also](https://tools.ietf.org/html/rfc7519#section-2)) to authentify users.

Example, showing minimal required set of fields in token payload:
```json
{
  "sub": "mtm@mtmco.net",
  "name": "mmeharga",
  "aud": "dispatcher",
  "roles": "magic,antares",
  "exp": 1613662947,
  "iss": "drupal",
  "iat": 1613662847,
  "tem": 1800
}
```

Where "sub" field is unique user identified, defined as email.


| Description | token field | drupal field |
| :--  | :-- | :-- |
| email |  sub   | ? | 
| full name |  name   | ? | 
| roles, coma-separated | roles |  ? |
| expiration | exp | ? |
| issued-at | iat | ? |
| issued-dy | iss | none |
| audience, issued-for | aud | none |
| email timeout | tem | none |

Specific sites may specify more roles, as needed.


## Origin of Tokens

We create JWT in [drupal](https://github.com/oda-hub/frontend-chart), using symmetric encryption (currently), secret injected [at the deployment](https://github.com/oda-hub/frontend-chart/issues/7)

Dispatcher may also create tokens for API, especially for  creating reduced scope tokens for passing to backends.

## Propagating the Tokens

JWT will be provided by frontend in each request, once the user is logged in. In particular:
* the cookie Drupal.visitor.token is defined and it contains a JWT
* the _oauth2_token will be defined for OAUTH2-secured API endpoints.
* the token is also provided as a paramter, <em>token</em>, within the URL for each request to the dispatcher
* the token is also sent in the request header Authorization.

In the event of a public request, so one executed with no user logged in, the token won't be provided.

JWT [will be also](https://github.com/oda-hub/frontend-astrooda/issues/1) made available to the user for use in the API

## Actual roles

But they See current working definition of real roles [here](https://github.com/oda-hub/doc-multi-user/blob/main/plan-roles-users.md).

## Other Applicable Documents

|||
| :-- | :-- |
| Meetings | [2021-02-22](https://github.com/oda-hub/meetings/blob/main/2021-02-22/MoM.md) |


