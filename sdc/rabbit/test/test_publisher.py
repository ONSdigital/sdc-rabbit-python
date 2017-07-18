import logging
import unittest
from unittest import mock

from pika.exceptions import AMQPConnectionError, NackError, UnroutableError

from sdc.rabbit import QueuePublisher
from sdc.rabbit.exceptions import PublishMessageError
from sdc.rabbit.test.test_data import test_data


class TestPublisher(unittest.TestCase):
    logger = logging.getLogger(__name__)

    publisher = QueuePublisher(['amqp://guest:guest@0.0.0.0:5672',
                                'amqp://guest:guest@0.0.0.0:5672'],
                               'test')

    bad_publisher = QueuePublisher(['amqp://guest:guest@0.0.0.0:672',
                                    'amqp://guest:guest@0.0.0.0:672'],
                                   'test')

    def test_init(self):
        this_publisher = QueuePublisher(['amqp://guest:guest@0.0.0.0:5672',
                                         'amqp://guest:guest@0.0.0.0:5672'],
                                        'test')
        self.assertEqual(this_publisher._urls, ['amqp://guest:guest@0.0.0.0:5672',
                                                'amqp://guest:guest@0.0.0.0:5672'])
        self.assertEqual(this_publisher._queue, 'test')
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)

    def test_connect_loops_correctly(self):
        this_publisher = QueuePublisher(['amqp://guest:guest@0.0.0.0:672',
                                         'amqp://guest:guest@0.0.0.0:5672'],
                                        'test')

        self.assertEqual(this_publisher._urls, ['amqp://guest:guest@0.0.0.0:672',
                                                'amqp://guest:guest@0.0.0.0:5672'])
        self.assertEqual(this_publisher._queue, 'test')
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)

        this_publisher._connect()

    def test_connect_amqp_connection_error(self):
        with self.assertRaises(AMQPConnectionError):
            self.bad_publisher._connect()

    def test_connect_amqpok(self):

        result = self.publisher._connect()
        self.assertEqual(result, True)

    def test_disconnect_ok(self):
        self.publisher._connect()

        with self.assertLogs(level='DEBUG') as cm:
            self.publisher._disconnect()

        msg = 'Disconnected from queue'
        self.assertIn(msg, cm[1][-1])

    def test_disconnect_error(self):

        self.publisher._disconnect()
        with self.assertLogs(level='ERROR') as cm:
            self.publisher._disconnect()

        msg = 'Unable to close connection'
        self.assertIn(msg, cm.output[0])

    def test_publish_message_no_connection(self):
        with self.assertRaises(PublishMessageError):
            self.bad_publisher.publish_message(test_data['valid'])

    def test_publish(self):
        self.publisher._connect()
        result = self.publisher.publish_message(test_data['valid'])
        self.assertEqual(None, result)

    def test_publish_nack_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = NackError('a')
            self.publisher._connect()
            with self.assertRaises(PublishMessageError):
                self.publisher.publish_message(test_data['valid'])

    def test_publish_unroutable_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = UnroutableError('a')
            self.publisher._connect()
            with self.assertRaises(PublishMessageError):
                self.publisher.publish_message(test_data['valid'])

    def test_publish_generic_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = Exception()
            self.publisher._connect()
            with self.assertRaises(Exception):
                self.publisher.publish_message(test_data['valid'])
