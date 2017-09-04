import json
import logging
import unittest
from unittest import mock

from sdc.rabbit import MessageConsumer, QueuePublisher
from sdc.rabbit.exceptions import BadMessageError, RetryableError
from sdc.rabbit.exceptions import PublishMessageError, QuarantinableError


class DotDict(dict):

    __getattr__ = dict.get


class TestSdxConsumer(unittest.TestCase):
    logger = logging.getLogger(__name__)

    def setUp(self):
        self.amqp_url = 'amqp://guest:guest@0.0.0.0:5672'

        self.quarantine_publisher = QueuePublisher([self.amqp_url],
                                                   'test_quarantine')

        self.consumer = MessageConsumer(True,
                                        'test',
                                        'topic',
                                        'test',
                                        [self.amqp_url],
                                        self.quarantine_publisher,
                                        lambda x, y: True)

        self.props = DotDict({'headers': {'tx_id': 'test'}})
        self.props_no_tx_id = DotDict({'headers': {}})
        self.props_no_x_delivery_count = DotDict({'headers': {'tx_id': 'test'}})
        self.basic_deliver = DotDict({'delivery_tag': 'test'})
        self.body = json.loads('"{test message}"')

    def test_callable(self):
        with self.assertRaises(AttributeError):
            self.consumer = MessageConsumer(True,
                                            'test',
                                            'topic',
                                            'test',
                                            [self.amqp_url],
                                            self.quarantine_publisher,
                                            self.body)

    def test_queue_attributes(self):
        self.assertEqual(self.consumer._exchange, 'test')
        self.assertEqual(self.consumer._exchange_type, 'topic')
        self.assertEqual(self.consumer._queue, 'test')
        self.assertEqual(self.consumer._rabbit_urls,
                         ['amqp://guest:guest@0.0.0.0:5672'])
        self.assertEqual(self.consumer._durable_queue, True)

        self.assertEqual(self.consumer.quarantine_publisher._queue,
                         'test_quarantine')
        self.assertEqual(self.consumer.quarantine_publisher._urls,
                         ['amqp://guest:guest@0.0.0.0:5672'])

    def test_tx_id(self):
        self.assertEqual('test', self.consumer.tx_id(self.props))

        with self.assertRaises(KeyError):
            self.consumer.tx_id(self.props_no_tx_id)

    def test_on_message_txid_key_error_returns_none(self):
        mock_method = 'sdc.rabbit.AsyncConsumer.reject_message'
        with mock.patch(mock_method) as barMock:
            barMock.return_value = None
            result = self.consumer.on_message(self.consumer._channel,
                                              self.basic_deliver,
                                              self.props_no_tx_id,
                                              'test')

        self.assertEqual(result, None)

    def test_on_message_logger(self):
        mock_method = 'sdc.rabbit.AsyncConsumer.acknowledge_message'
        with mock.patch(mock_method) as bar_mock:
            bar_mock.return_value = None
            result = self.consumer.on_message(self.consumer._channel,
                                              self.basic_deliver,
                                              self.props,
                                              self.body.encode('UTF-8'))
            self.assertEqual(None, result)

    def test_on_message_quarantinable_error(self):
        mock_method = 'sdc.rabbit.AsyncConsumer.reject_message'
        with mock.patch(mock_method):
            self.consumer.process = lambda x: (_ for _ in ()).throw(QuarantinableError())
            with self.assertLogs(level='ERROR') as cm:
                result = self.consumer.on_message(self.consumer._channel,
                                                  self.basic_deliver,
                                                  self.props,
                                                  self.body.encode('UTF-8'))
        self.assertEqual(result, None)
        self.assertIn("Quarantinable error occured", cm[0][1].message)

    def test_on_message_publish_message_error(self):
        mock_reject_message = 'sdc.rabbit.AsyncConsumer.reject_message'
        mock_publish_message = 'sdc.rabbit.QueuePublisher.publish_message'
        with mock.patch(mock_reject_message):
            self.consumer.process = lambda x: (_ for _ in ()).throw(QuarantinableError())
            with mock.patch(mock_publish_message) as publish_mock:
                publish_mock.side_effect = PublishMessageError
                with self.assertLogs(level='ERROR') as cm:
                    result = self.consumer.on_message(self.consumer._channel,
                                                      self.basic_deliver,
                                                      self.props,
                                                      self.body.encode('UTF-8'))
        self.assertEqual(result, None)

        expected_msg = "Unable to publish message to quarantine queue. Rejecting message and requeing."
        self.assertIn(expected_msg, cm[0][1].message)

    def test_on_message_bad_message_error(self):
        mock_method = 'sdc.rabbit.AsyncConsumer.reject_message'

        def bad_message_error(x, y):
            raise BadMessageError

        with mock.patch(mock_method):
            self.consumer.process = bad_message_error
            with self.assertLogs(level='ERROR') as cm:
                result = self.consumer.on_message(self.consumer._channel,
                                                  self.basic_deliver,
                                                  self.props,
                                                  self.body.encode('UTF-8'))
        self.assertEqual(result, None)
        self.assertIn("Bad message", cm[0][0].message)

    def test_on_message_retryable_message_error(self):
        mock_method = 'sdc.rabbit.AsyncConsumer.nack_message'

        def retryable_error(x, y):
            raise RetryableError

        with mock.patch(mock_method):
            self.consumer.process = retryable_error
            with self.assertLogs(level='ERROR') as cm:
                result = self.consumer.on_message(self.consumer._channel,
                                                  self.basic_deliver,
                                                  self.props,
                                                  self.body.encode('UTF-8'))
        self.assertEqual(result, None)

        self.assertIn("Failed to process", cm[0][0].message)
