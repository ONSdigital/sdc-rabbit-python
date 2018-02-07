import logging
import unittest
from unittest import mock

from pika.exceptions import AMQPConnectionError, NackError, UnroutableError

from sdc.rabbit import ExchangePublisher, QueuePublisher
from sdc.rabbit.exceptions import PublishMessageError
from sdc.rabbit.test.test_data import test_data


class TestPublisher(unittest.TestCase):
    logger = logging.getLogger(__name__)

    q_publisher = QueuePublisher(['amqp://guest:guest@0.0.0.0:5672',
                                 'amqp://guest:guest@0.0.0.0:5672'],
                                 'test')

    bad_q_publisher = QueuePublisher(['amqp://guest:guest@0.0.0.0:672',
                                     'amqp://guest:guest@0.0.0.0:672'],
                                     'test')

    confirm_delivery_q_publisher = QueuePublisher(['amqp://guest:guest@0.0.0.0:5672',
                                                  'amqp://guest:guest@0.0.0.0:5672'],
                                                  'test',
                                                  confirm_delivery=True)

    x_publisher = ExchangePublisher(['amqp://guest:guest@0.0.0.0:5672',
                                    'amqp://guest:guest@0.0.0.0:5672'],
                                    'test')

    bad_x_publisher = ExchangePublisher(['amqp://guest:guest@0.0.0.0:672',
                                        'amqp://guest:guest@0.0.0.0:672'],
                                        'test')

    confirm_delivery_x_publisher = ExchangePublisher(['amqp://guest:guest@0.0.0.0:5672',
                                                     'amqp://guest:guest@0.0.0.0:5672'],
                                                     'test',
                                                     confirm_delivery=True)

    def test_incomplete_publisher(self):
        from sdc.rabbit.publishers import Publisher

        class BadPublisher(Publisher):
            pass

        this_publisher = BadPublisher(['amqp://guest:guest@0.0.0.0:5672'])

        with self.assertRaises(NotImplementedError):
            this_publisher._do_publish('test')
        with self.assertRaises(NotImplementedError):
            this_publisher._declare()
        with self.assertRaises(PublishMessageError):
            this_publisher.publish_message('test')

    def test_queue_init(self):
        this_publisher = QueuePublisher(['amqp://guest:guest@0.0.0.0:5672',
                                        'amqp://guest:guest@0.0.0.0:5672'],
                                        'test')
        self.assertEqual(this_publisher._urls, ['amqp://guest:guest@0.0.0.0:5672',
                                                'amqp://guest:guest@0.0.0.0:5672'])
        self.assertEqual(this_publisher._queue, 'test')
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)

    def test_exchange_init(self):
        this_publisher = ExchangePublisher(['amqp://guest:guest@0.0.0.0:5672',
                                           'amqp://guest:guest@0.0.0.0:5672'],
                                           'test')
        self.assertEqual(this_publisher._urls, ['amqp://guest:guest@0.0.0.0:5672',
                                                'amqp://guest:guest@0.0.0.0:5672'])
        self.assertEqual(this_publisher._exchange, 'test')
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)

    def test_queue_connect_loops_correctly(self):
        this_publisher = QueuePublisher(['amqp://guest:guest@0.0.0.0:672',
                                        'amqp://guest:guest@0.0.0.0:5672'],
                                        'test')

        self.assertEqual(this_publisher._urls, ['amqp://guest:guest@0.0.0.0:672',
                                                'amqp://guest:guest@0.0.0.0:5672'])
        self.assertEqual(this_publisher._queue, 'test')
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)

        self.assertTrue(this_publisher._connect())

    def test_exchange_connect_loops_correctly(self):
        this_publisher = ExchangePublisher(['amqp://guest:guest@0.0.0.0:672',
                                           'amqp://guest:guest@0.0.0.0:5672'],
                                           'test')

        self.assertEqual(this_publisher._urls, ['amqp://guest:guest@0.0.0.0:672',
                                                'amqp://guest:guest@0.0.0.0:5672'])
        self.assertEqual(this_publisher._exchange, 'test')
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)

        self.assertTrue(this_publisher._connect())

    def test_queue_connect_amqp_connection_error(self):
        with self.assertRaises(AMQPConnectionError):
            self.bad_q_publisher._connect()

    def test_queue_connect_confirm_delivery_true(self):
        with self.assertLogs(level='INFO') as cm:
            self.confirm_delivery_q_publisher._connect()

        msg = 'Enabled delivery confirmation'
        self.assertIn(msg, cm[0][3].msg)

    def test_exchange_connect_amqp_connection_error(self):
        with self.assertRaises(AMQPConnectionError):
            self.bad_x_publisher._connect()

    def test_exchange_connect_confirm_delivery_true(self):
        with self.assertLogs(level='INFO') as cm:
            self.confirm_delivery_x_publisher._connect()

        msg = 'Enabled delivery confirmation'
        self.assertIn(msg, cm[0][3].msg)

    def test_queue_connect_amqpok(self):
        result = self.q_publisher._connect()
        self.assertEqual(result, True)

    def test_queue_disconnect_ok(self):
        self.q_publisher._connect()

        with self.assertLogs(level='DEBUG') as cm:
            self.q_publisher._disconnect()

        msg = 'Disconnected from rabbit'
        self.assertIn(msg, cm[1][-1])

    def test_exchange_connect_amqpok(self):
        result = self.x_publisher._connect()
        self.assertEqual(result, True)

    def test_exchange_disconnect_ok(self):
        self.x_publisher._connect()

        with self.assertLogs(level='DEBUG') as cm:
            self.x_publisher._disconnect()

        msg = 'Disconnected from rabbit'
        self.assertIn(msg, cm[1][-1])

    def test_queue_disconnect_already_closed_connection(self):
        self.q_publisher._connect()
        self.q_publisher._disconnect()
        with self.assertLogs(level='DEBUG') as cm:
            self.q_publisher._disconnect()

        msg = 'Close called on closed connection'
        self.assertIn(msg, cm.output[0])

    def test_exchange_disconnect_already_closed_connection(self):
        self.x_publisher._connect()
        self.x_publisher._disconnect()
        with self.assertLogs(level='DEBUG') as cm:
            self.x_publisher._disconnect()

        msg = 'Close called on closed connection'
        self.assertIn(msg, cm.output[0])

    def test_queue_publish_message_no_connection(self):
        with self.assertRaises(PublishMessageError):
            self.bad_q_publisher.publish_message(test_data['valid'])

    def test_queue_publish(self):
        self.q_publisher._connect()
        with self.assertLogs(level='INFO') as cm:
            result = self.q_publisher.publish_message(test_data['valid'])
            self.assertEqual(True, result)
        self.assertIn('Published message to queue', cm.output[3])

    def test_queue_publish_nack_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = NackError('a')
            self.q_publisher._connect()
            with self.assertRaises(PublishMessageError):
                self.q_publisher.publish_message(test_data['valid'])

    def test_queue_publish_unroutable_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = UnroutableError('a')
            self.q_publisher._connect()
            with self.assertRaises(PublishMessageError):
                self.q_publisher.publish_message(test_data['valid'])

    def test_queue_publish_generic_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = Exception()
            self.q_publisher._connect()
            with self.assertRaises(Exception):
                self.q_publisher.publish_message(test_data['valid'])

    def test_exchange_publish_message_no_connection(self):
        with self.assertRaises(PublishMessageError):
            self.bad_x_publisher.publish_message(test_data['valid'])

    def test_exchange_publish(self):
        self.x_publisher._connect()
        with self.assertLogs(level='INFO') as cm:
            result = self.x_publisher.publish_message(test_data['valid'])
            self.assertEqual(True, result)
        self.assertIn('Published message to exchange', cm.output[3])

    def test_exchange_publish_nack_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = NackError('a')
            self.x_publisher._connect()
            with self.assertRaises(PublishMessageError):
                self.x_publisher.publish_message(test_data['valid'])

    def test_exchange_publish_unroutable_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = UnroutableError('a')
            self.x_publisher._connect()
            with self.assertRaises(PublishMessageError):
                self.x_publisher.publish_message(test_data['valid'])

    def test_exchange_publish_generic_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = Exception()
            self.x_publisher._connect()
            with self.assertRaises(Exception):
                self.x_publisher.publish_message(test_data['valid'])
