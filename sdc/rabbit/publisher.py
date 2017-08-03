import logging

import pika
from pika.exceptions import NackError, UnroutableError
from structlog import wrap_logger

from sdc.rabbit.exceptions import PublishMessageError

logger = logging.getLogger(__name__)
logger = wrap_logger(logger)


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
                logger.debug("Connected to queue")
                return True

            except pika.exceptions.AMQPConnectionError as e:
                logger.error("Unable to connect to queue",
                             exception=repr(e))
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

        except Exception as e:
            logger.error("Unable to close connection", exception=repr(e))

    def publish_message(self, message, content_type=None, headers=None):
        """
        Publish a response message to a RabbitMQ queue.

        :param message: Response message
        :param content_type: Pika BasicProperties content_type value
        :param headers: Message header properties

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
            self._channel.basic_publish(exchange='',
                                        routing_key=self._queue,
                                        properties=pika.BasicProperties(
                                            content_type=content_type,
                                            headers=headers,
                                            delivery_mode=2
                                        ),
                                        body=message)
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
            logger.error("Unknown exception occured. Message not published.")
            raise PublishMessageError
        msg = 'Published message to {} queue'
        logger.error(msg.format(self._queue))
