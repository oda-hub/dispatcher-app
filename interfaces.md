# Purpose

# User token origin and usage

We use [JWT](https://jwt.io/introduction/) to authentify users.

We create JWT in [drupal](https://github.com/oda-hub/frontend-chart), using symmetric encrytion (currently), secret injected [at the deployment](https://github.com/oda-hub/frontend-chart/issues/7)

JWT will be provided by frontend in each request, once the user is logged in.
* the cookie Drupal.visitor.token is defined and it contains a JWT
* The token is also provided as a paramter, token, in the URL for each request to the dispatcher as
* Token is also sent in request header Authorization

JWT [will be also](https://github.com/oda-hub/frontend-astrooda/issues/1) made available to the user for use in the API

The payload part of the token contains the user email, name, roles and the expiration time of the token.

Example:
```json
{
  "email": "mtm@mtmco.net",
  "name": "mmeharga",
  "roles": "authenticated user, content manager, general, magic",
  "exp": 1613662947
}
```

exp is the expiration time of the token which can be defined in the Drupal administration GUI as a life time in minutes.
