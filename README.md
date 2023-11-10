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

It is now possible to receive update messages regarding the status of the jobs submitted on the mmoda platform. An emailing system is already provided out-of-the-box, provided that a token is used (this contains the user email address to which the updates will be sent, along with a number of other configurations, more details [here](interfaces.md#user-tokens)).

The platform supports now also the sending of messages via the [Matrix](https://matrix.org/) platform, and a dedicated messaging bot (__@mmoda-bot__) is available for receiving jobs updates messages.

All communication over Matrix happens within a **room**: for this reason, one must be configured.

Two options are possible:
* _**shared room**_: a room where multiple users can participate.  
* _**direct message room**_: where the messages are directly sent and received, to and from a particular user.

Obviously, an account on the matrix platform is necessary. In addition, a number of configuration steps are necessary, either on the token as well as on the matrix user account.

For the purpose of this documentation, the matrix client **element** will be used (https://element.io/).

Matrix account configuration
----------------------------

### Configuration of a shared room

Within the element client:
* Create a dedicated room 
<br/>
<div align="center">
<img align="center" src="readme_imgs/img.png" width ="45%">
<img align="center" src="readme_imgs/img_1.png"  width ="45%">
</div>
<br clear="left"/>

* Invite @mmoda-bot to the room created in the previous step
<div align="center">
<img align="center" src="readme_imgs/img_2.png" width ="45%">
<img align="center" src="readme_imgs/img_3.png" width ="45%">
</div>
<br clear="left"/>

* @mmoda-bot will then have to join the room (as stated within the [client-server-api](https://spec.matrix.org/latest/client-server-api/#room-membership)), and this will be automatically performed by the dispatcher.

An example of a message is in the image below:

![img.png](readme_imgs/img_matrix_msg_example.png)

Token specific configuration
----------------------------

Within the token, some configuration are user-specific (and mandatory), whereas others are optional, and if not provided, a set of default values will be provided within the dispatcher configuration:

* `mxroomid` id of the room to where the messages should be sent (can be a room for direct messages, or a room used specifically for the purpose of mmoda messages)

