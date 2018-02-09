import logging

import pika
from pika.exceptions import NackError, UnroutableError

from sdc.rabbit.exceptions import PublishMessageError

logger = logging.getLogger(__name__)


class Publisher(object):
    """Base class for publishers to RabbitMQ."""

    def __init__(self, urls, **kwargs):
        """Create a new instance of a Publisher class

        :param urls: List of RabbitMQ cluster URLs
        :param confirm_delivery: Delivery confirmations toggle
        :param **kwargs: Custom key/value pairs passed to the arguments
            parameter of pika's channel.exchange_declare method

        :returns: Object of type Publisher
        :rtype: ExchangePublisher

        """
        self._urls = urls
        self._arguments = kwargs
        self._connection = None
        self._channel = None
        self._confirm_delivery = False
        if 'confirm_delivery' in kwargs:
            self._confirm_delivery = True
            self._arguments.pop('confirm_delivery', None)

    def _declare(self):
        raise NotImplementedError('_declare not implemented')

    def _connect(self):
        """
        Connect to a RabbitMQ instance

        :returns: Boolean corresponding to success of connection
        :rtype: bool

        """
        logger.info("Connecting to rabbit")
        for url in self._urls:
            try:
                self._connection = pika.BlockingConnection(pika.URLParameters(url))
                self._channel = self._connection.channel()
                self._declare()
                if self._confirm_delivery:
                    self._channel.confirm_delivery()
                    logger.info("Enabled delivery confirmation")
                logger.debug("Connected to rabbit")
                return True

            except pika.exceptions.AMQPConnectionError:
                logger.exception("Unable to connect to rabbit")
                continue
            except Exception:
                logger.exception("Unexpected exception connecting to rabbit")
                continue

        raise pika.exceptions.AMQPConnectionError

    def _disconnect(self):
        """
        Cleanly close a RabbitMQ connection.

        :returns: None

        """
        try:
            self._connection.close()
            logger.debug("Disconnected from rabbit")
        except Exception:
            logger.exception("Unable to close connection")

    def _do_publish(self, message, mandatory=False, immediate=False, content_type=None, headers=None):
        raise NotImplementedError('_do_publish not implemented')

    def publish_message(self, message, content_type=None, headers=None, mandatory=False, immediate=False):
        """
        Publish a response message to a RabbitMQ instance.

        :param message: Response message
        :param content_type: Pika BasicProperties content_type value
        :param headers: Message header properties
        :param mandatory: The mandatory flag
        :param immediate: The immediate flag

        :returns: Boolean corresponding to the success of publishing
        :rtype: bool

        """
        logger.debug("Publishing message")
        try:
            self._connect()
            return self._do_publish(mandatory=mandatory,
                                    immediate=immediate,
                                    content_type=content_type,
                                    headers=headers,
                                    message=message)
        except pika.exceptions.AMQPConnectionError:
            logger.error("AMQPConnectionError occurred. Message not published.")
            raise PublishMessageError
        except NackError:
            # raised when a message published in publisher-acknowledgments mode
            # is returned via `Basic.Return` followed by `Basic.Ack`.
            logger.error("NackError occurred. Message not published.")
            raise PublishMessageError
        except UnroutableError:
            # raised when a message published in publisher-acknowledgments
            # mode is returned via `Basic.Return` followed by `Basic.Ack`.
            logger.error("UnroutableError occurred. Message not published.")
            raise PublishMessageError
        except Exception:
            logger.exception("Unknown exception occurred. Message not published.")
            raise PublishMessageError


class ExchangePublisher(Publisher):
    """This is an exchange publisher that publishes response messages to a
    RabbitMQ exchange.
    """
    _durable_exchange = False

    def __init__(self, urls, exchange, exchange_type='fanout', **kwargs):
        """Create a new instance of the ExchangePublisher class

        :param urls: List of RabbitMQ cluster URLs
        :param exchange: Exchange name
        :param exchange_type: Type of exchange to declare
        :param confirm_delivery: Delivery confirmations toggle
        :param **kwargs: Custom key/value pairs passed to the arguments
            parameter of pika's channel.exchange_declare method

        :returns: Object of type ExchangePublisher
        :rtype: ExchangePublisher

        """
        self._exchange = exchange
        self._exchange_type = exchange_type
        super(ExchangePublisher, self).__init__(urls, **kwargs)

    def _declare(self):
        self._channel.exchange_declare(exchange=self._exchange,
                                       exchange_type=self._exchange_type,
                                       durable=self._durable_exchange,
                                       arguments=self._arguments)

    def _do_publish(self, message, mandatory=False, immediate=False, content_type=None, headers=None):
        result = self._channel.basic_publish(exchange=self._exchange,
                                             routing_key='',
                                             mandatory=mandatory,
                                             immediate=immediate,
                                             properties=pika.BasicProperties(
                                                 content_type=content_type,
                                                 headers=headers,
                                                 delivery_mode=2
                                             ),
                                             body=message)
        logger.info('Published message to exchange exchange={}'.format(self._exchange))
        return result


class DurableExchangePublisher(Publisher):
    """This is an exchange publisher that publishes response messages to a
    (durable - survives a reboot) RabbitMQ exchange.
    """
    _durable_exchange = True


class QueuePublisher(Publisher):
    """This is a queue publisher that publishes response messages to a
    RabbitMQ queue.

    """
    _durable_queue = True

    def __init__(self, urls, queue, **kwargs):
        """Create a new instance of the QueuePublisher class

        :param urls: List of RabbitMQ cluster URLs.
        :param queue: Queue name
        :param confirm_delivery: Delivery confirmations toggle
        :param **kwargs: Custom key/value pairs passed to the arguments
            parameter of pika's channel.queue_declare method

        :returns: Object of type QueuePublisher
        :rtype: QueuePublisher

        """
        self._queue = queue
        super(QueuePublisher, self).__init__(urls, **kwargs)

    def _declare(self):
        self._channel.queue_declare(queue=self._queue,
                                    durable=self._durable_queue,
                                    arguments=self._arguments)

    def _do_publish(self, message, mandatory=False, immediate=False, content_type=None, headers=None):
        result = self._channel.basic_publish(exchange='',
                                             routing_key=self._queue,
                                             mandatory=mandatory,
                                             immediate=immediate,
                                             properties=pika.BasicProperties(
                                                 content_type=content_type,
                                                 headers=headers,
                                                 delivery_mode=2
                                             ),
                                             body=message)
        logger.info('Published message to queue queue={}'.format(self._queue))
        return result
