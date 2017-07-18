import logging

from structlog import wrap_logger

from sdc.rabbit.exceptions import BadMessageError, RetryableError
from sdc.rabbit.exceptions import PublishMessageError, QuarantinableError

logger = wrap_logger(logging.getLogger(__name__))


class MessageConsumer():
    """This is a queue consumer that handles messages from RabbitMQ message queues.

    On receipt of a message it takes a number of params from the message
    properties, processes the message, and (if successful) positively
    acknowledges the publishing queue.

    If a message is not successfuly processed, it can be either negatively
    acknowledged, rejected or quarantined, depending on the type of excpetion
    raised.

    """
    @staticmethod
    def delivery_count(properties):
        """
        Returns the delivery count for a message from the rabbit queue. The
        value is auto-set by rabbitmq. Will raise KeyError if x-delivery-count
        is missing from message headers.


        :param properties: Message properties

        :returns: Delivery count value
        :rtype: int

        """
        delivery_count = properties.headers['x-delivery-count']
        return delivery_count + 1

    @staticmethod
    def tx_id(properties):
        """
        Gets the tx_id for a message from a rabbit queue, using the
        message properties. Will raise KeyError if tx_id is missing from message
        headers.

        : param properties: Message properties

        : returns: tx_id of survey response
        : rtype: str
        """
        tx = properties.headers['tx_id']
        logger.info("Retrieved tx_id from message properties: tx_id={}".format(tx))
        return tx

    def __init__(self, consumer, quarantine_publisher, process):
        """Create a new instance of the MessageConsumer class

        : param consumer: Object of type sdc.rabbit.AsyncConsumer
        : param process: Function or method to use for processsing message. Will
            be passed the body of the message as a string decoded using UTF - 8.
            Should raise sdc.rabbit.DecryptError, sdc.rabbit.BadMessageError or
            sdc.rabbit.RetryableError on failure, depending on the failure mode.

        :returns: Object of type Consumer
        :rtype: Consumer

        """
        self._consumer = consumer
        self._process = process
        self._quarantine_publisher = quarantine_publisher

        if not callable(process):
            msg = 'process callback is not callable'
            raise AttributeError(msg.format(process))

    def on_message(self, basic_deliver, properties, body):
        """Called on receipt of a message from a queue.

        Processes the message using the self._process method or function and positively
        acknowledges the queue if successful. If processing is not succesful,
        the message can either be rejected, quarantined or negatively acknowledged,
        depending on the failure mode.

        :param basic_deliver: AMQP basic.deliver method
        :param properties: Message properties
        :param body: Message body

        :returns: None

        """
        try:
            delivery_count = self.delivery_count(properties)
        except KeyError as e:
            self._consumer.reject_message(basic_deliver.delivery_tag)
            msg = 'Bad message properties - no delivery count'
            logger.error(msg, action="rejected", exception=str(e))
            return None

        try:
            tx_id = self.tx_id(properties)

            logger.info('Received message',
                        queue=self._consumer._queue,
                        delivery_tag=basic_deliver.delivery_tag,
                        delivery_count=delivery_count,
                        app_id=properties.app_id,
                        tx_id=tx_id)

        except KeyError as e:
            self._consumer.reject_message(basic_deliver.delivery_tag)
            logger.error("Bad message properties - no tx_id",
                         action="rejected",
                         exception=str(e),
                         delivery_count=delivery_count)
            return None

        try:
            self._process(body.decode("utf-8"))
            self._consumer.acknowledge_message(basic_deliver.delivery_tag,
                                               tx_id=tx_id)

        except QuarantinableError as e:
            # Throw it into the quarantine queue to be dealt with
            try:
                self._quarantine_publisher.publish_message(body)
                self._consumer.reject_message(basic_deliver.delivery_tag, tx_id=tx_id)
                logger.error("Quarantinable error occured",
                             action="quarantined",
                             exception=str(e),
                             tx_id=tx_id,
                             delivery_count=delivery_count)
            except PublishMessageError as e:
                logger.error("Unable to publish message to quarantine queue." +
                             " Rejecting message and requeing.")
                self._consumer.reject_message(basic_deliver.delivery_tag,
                                              requeue=True,
                                              tx_id=tx_id)

        except BadMessageError as e:
            # If it's a bad message then we have to reject it
            self._consumer.reject_message(basic_deliver.delivery_tag, tx_id=tx_id)
            logger.error("Bad message",
                         action="rejected",
                         exception=str(e),
                         tx_id=tx_id,
                         delivery_count=delivery_count)

        except RetryableError as e:
            self._consumer.nack_message(basic_deliver.delivery_tag, tx_id=tx_id)
            logger.error("Failed to process",
                         action="nack",
                         exception=str(e),
                         tx_id=tx_id,
                         delivery_count=delivery_count)
