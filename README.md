ODA Dispatcher
==========================================

[![codecov](https://codecov.io/gh/oda-hub/dispatcher-app/branch/master/graph/badge.svg?token=9A4QWsQNOo)](https://codecov.io/gh/oda-hub/dispatcher-app)

A flexible python framework to bridge front-end and data-server for scientific online analysis of astrophysical data:

* provides boilerplate code to implement interfaces to specific instrument backends with [plugins](dispatcher-plugins).
* implements interface to [frontend](frontend).
* explains [auth](interfaces.md)

**this repository also contains much of relevant documentations to gluing these levels together**

What's the license?
-------------------

dispatcher-app is distributed under the terms of [The MIT License](LICENSE).

Who's responsible?
-------------------
Andrea Tramacere, Volodymyr Savchenko

Astronomy Department of the University of Geneva, Chemin d'Ecogia 16, CH-1290 Versoix, Switzerland


Jobs updates with messages on matrix
-----------------------------------------------

In order to receive update messages regarding the status of the jobs submitted on the mmoda platform, 
an emailing system is already provided out-of-the-box, provided that a token used 
(this contains the user email address to which the updates will be sent).

The platform supports now also the sending of messages via the [Matrix](https://matrix.org/) platform.

To enable those, a number of configuration steps are necessary:

* Create a room
* Invite @mmoda-bot to it
  * @mmoda-bot has to join the room
* Update the token with the necessary fields
  * Customize the token if wished
