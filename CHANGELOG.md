

async mode

* ensures that every request is fast, ~600ms currently, up to 60s before
* compatible with <1.1.0 api when no async is used
* reduces load on dispatcher by queueing requests
* asyncs slow and fast backends as well postprocessing
* helps to send many queries at once fast - works well with async api to submit batches of jobs
* helps to retrieve many queries at once fast - to retrieve batches of results. TODO: sometimes transport is still slow, limiting?
