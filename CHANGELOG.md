# Unreleased

### 1.5.0 2018-02-09
 - Add additional publisher for handling fanouts to a durable exchange

### 1.4.0 2018-02-08
 - Add additional publisher for handling fanouts to an exchange

### 1.3.1 2018-01-05
 - Add additional exception handling in _connect of consumer

### 1.3.0 2018-01-04
 - Additional exception handling in on_message of consumer
 - Update Pika to 0.11.2

### 1.2.1 2017-12-15
 - Copy message headers to the quarantine queue and quarantine rather than reject bad messages

### 1.2.0 2017-10-30
 - Log exceptions using `logging.Logger.exception` method in consumer module

### 1.1.0 2017-10-27
- Fix typo in log line in consumer module
- Do not restart ioloop if stop method is called on consumer
- Better exception handling of TypeErrors in consumer module when the `headers` attribute was not set in a message's properties.

### 1.0.2 2017-09-07
- Bug fix release. Removes an issue where an additional keyword argument was being passed to the logger, causing an exception.

### 1.0.1 2017-09-07
- Add configurable mandatory and immediate flags for basic publish
- Add configurable delivery confirmation for channel

### 1.0.0 2017-08-30
- Stabilised API

### 0.3.4
- Remove call to ioloop.stop in reconnect handler.

### 0.3.3
- Remove call to `logging.basicConfig`

### 0.3.2
- Remove x-delivery-count checking

### 0.3.1
- Add tornado to requirements file
- Pass tx_id in call to process method

### 0.3.0
- Move the data model to inheritence instead of composition
- Add a TornadoConsumer class that uses a TornadoConnection object to connect to RabbitMQ

### 0.1.0
- Initial release
