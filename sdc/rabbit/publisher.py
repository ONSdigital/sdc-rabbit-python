import logging

import pika
from pika.exceptions import NackError, UnroutableError

from sdc.rabbit.exceptions import PublishMessageError

logger = logging.getLogger(__name__)


class QueuePublisher(object):
    """This is a queue publisher that publishes response messages to a
    RabbitMQ queue.

    """
    _durable_queue = True

    def __init__(self, urls, queue, **kwargs):
        """Create a new instance of the QueuePublisher class

        :param logger: A reference to a logging.Logger instance
        :param urls: List of RabbitMQ cluster URLs.
        :param queue: Queue name
        :param confirm_delivery: Delivery confirmations toggle
        :param **kwargs: Custom key/value pairs passed to the arguments
            parameter of pika's channel.queue_declare method

        :returns: Object of type QueuePublisher
        :rtype: QueuePublisher

        """
        self._urls = urls
        self._queue = queue
        self._arguments = kwargs
        self._connection = None
        self._channel = None
        self._confirm_delivery = False
        if 'confirm_delivery' in kwargs:
            self._confirm_delivery = True
            self._arguments.pop('confirm_delivery', None)

    def _connect(self):
        """
        Connect to a RabbitMQ queue

        :returns: Boolean corresponding to success of connection
        :rtype: bool

        """
        logger.info("Connecting to queue")
        for url in self._urls:
            try:
                self._connection = pika.BlockingConnection(pika.URLParameters(url))
                self._channel = self._connection.channel()
                self._channel.queue_declare(queue=self._queue,
                                            durable=self._durable_queue,
                                            arguments=self._arguments)
                if self._confirm_delivery:
                    self._channel.confirm_delivery()
                    logger.info("Enabled delivery confirmation")
                logger.debug("Connected to queue")
                return True

            except pika.exceptions.AMQPConnectionError:
                logger.exception("Unable to connect to queue")
                continue

        raise pika.exceptions.AMQPConnectionError

    def _disconnect(self):
        """
        Cleanly close a RabbitMQ queue connection.

        :returns: None

        """
        try:
            self._connection.close()
            logger.debug("Disconnected from queue")

        except Exception:
            logger.exception("Unable to close connection")

    def publish_message(self, message, content_type=None, headers=None, mandatory=False, immediate=False):
        """
        Publish a response message to a RabbitMQ queue.

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
        except pika.exceptions.AMQPConnectionError:
            logger.error("Message not published. RetryableError raised")
            raise PublishMessageError

        try:
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

            logger.info('Published message to queue', queue=self._queue)
            return result
        except NackError:
            # raised when a message published in publisher-acknowledgments mode
            # is returned via `Basic.Return` followed by `Basic.Ack`.
            logger.error("NackError occured. Message not published.")
            raise PublishMessageError
        except UnroutableError:
            # raised when a message published in publisher-acknowledgments
            # mode is returned via `Basic.Return` followed by `Basic.Ack`.
            logger.error("UnroutableError occured. Message not published.")
            raise PublishMessageError
        except Exception:
            logger.exception("Unknown exception occured. Message not published.")
            raise PublishMessageError
