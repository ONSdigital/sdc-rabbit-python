import logging
import unittest
from unittest import mock

from pika.exceptions import AMQPConnectionError, NackError, UnroutableError

from sdc.rabbit import DurableExchangePublisher, ExchangePublisher, QueuePublisher
from sdc.rabbit.exceptions import PublishMessageError
from sdc.rabbit.test.test_data import test_data


good_urls = ['amqp://guest:guest@0.0.0.0:5672', 'amqp://guest:guest@0.0.0.0:5672']
bad_urls = ['amqp://guest:guest@0.0.0.0:672', 'amqp://guest:guest@0.0.0.0:672']
loop_urls = ['amqp://guest:guest@0.0.0.0:672', 'amqp://guest:guest@0.0.0.0:5672']

queue_name = 'test_queue'
exchange_name = 'test_exchange'
durable_exchange_name = 'test_durable'


class TestPublisher(unittest.TestCase):
    logger = logging.getLogger(__name__)

    queue_publisher = QueuePublisher(good_urls, queue_name)
    bad_queue_publisher = QueuePublisher(bad_urls, queue_name)
    confirm_delivery_queue_publisher = QueuePublisher(good_urls, queue_name, confirm_delivery=True)

    exchange_publisher = ExchangePublisher(good_urls, exchange_name)
    bad_exchange_publisher = ExchangePublisher(bad_urls, exchange_name)
    confirm_delivery_exchange_publisher = ExchangePublisher(good_urls, exchange_name, confirm_delivery=True)

    durable_exchange_publisher = DurableExchangePublisher(good_urls, durable_exchange_name)
    bad_durable_exchange_publisher = DurableExchangePublisher(bad_urls, durable_exchange_name)
    confirm_delivery_durable_exchange_publisher = DurableExchangePublisher(good_urls, durable_exchange_name, confirm_delivery=True)

    def test_incomplete_publisher(self):
        from sdc.rabbit.publishers import Publisher

        class BadPublisher(Publisher):
            pass

        this_publisher = BadPublisher(good_urls[:1])

        with self.assertRaises(NotImplementedError):
            this_publisher._do_publish('test')
        with self.assertRaises(NotImplementedError):
            this_publisher._declare()
        with self.assertRaises(PublishMessageError):
            this_publisher.publish_message('test')

    def test_queue_init(self):
        this_publisher = QueuePublisher(good_urls, queue_name)
        self.assertEqual(this_publisher._urls, good_urls)
        self.assertEqual(this_publisher._queue, queue_name)
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)
        self.assertEqual(this_publisher._durable_queue, True)

    def test_exchange_init(self):
        this_publisher = ExchangePublisher(good_urls, exchange_name)
        self.assertEqual(this_publisher._urls, good_urls)
        self.assertEqual(this_publisher._exchange, exchange_name)
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)
        self.assertEqual(this_publisher._durable_exchange, False)

    def test_durable_exchange_init(self):
        this_publisher = DurableExchangePublisher(good_urls, durable_exchange_name)
        self.assertEqual(this_publisher._urls, good_urls)
        self.assertEqual(this_publisher._exchange, durable_exchange_name)
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)
        self.assertEqual(this_publisher._durable_exchange, True)

    def test_queue_connect_loops_correctly(self):
        this_publisher = QueuePublisher(loop_urls, queue_name)

        self.assertEqual(this_publisher._urls, loop_urls)
        self.assertEqual(this_publisher._queue, queue_name)
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)

        self.assertTrue(this_publisher._connect())

    def test_exchange_connect_loops_correctly(self):
        this_publisher = ExchangePublisher(loop_urls, exchange_name)

        self.assertEqual(this_publisher._urls, loop_urls)
        self.assertEqual(this_publisher._exchange, exchange_name)
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)

        self.assertTrue(this_publisher._connect())

    def test_durable_exchange_connect_loops_correctly(self):
        this_publisher = DurableExchangePublisher(loop_urls, durable_exchange_name)

        self.assertEqual(this_publisher._urls, loop_urls)
        self.assertEqual(this_publisher._exchange, durable_exchange_name)
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)

        self.assertTrue(this_publisher._connect())

    def test_queue_connect_amqp_connection_error(self):
        with self.assertRaises(AMQPConnectionError):
            self.bad_queue_publisher._connect()

    def test_queue_connect_confirm_delivery_true(self):
        with self.assertLogs(level='INFO') as cm:
            self.confirm_delivery_queue_publisher._connect()

        msg = 'Enabled delivery confirmation'
        self.assertIn(msg, cm[0][3].msg)

    def test_exchange_connect_amqp_connection_error(self):
        with self.assertRaises(AMQPConnectionError):
            self.bad_exchange_publisher._connect()

    def test_exchange_connect_confirm_delivery_true(self):
        with self.assertLogs(level='INFO') as cm:
            self.confirm_delivery_exchange_publisher._connect()

        msg = 'Enabled delivery confirmation'
        self.assertIn(msg, cm[0][3].msg)

    def test_durable_exchange_connect_amqp_connection_error(self):
        with self.assertRaises(AMQPConnectionError):
            self.bad_durable_exchange_publisher._connect()

    def test_durable_exchange_connect_confirm_delivery_true(self):
        with self.assertLogs(level='INFO') as cm:
            self.confirm_delivery_durable_exchange_publisher._connect()

        msg = 'Enabled delivery confirmation'
        self.assertIn(msg, cm[0][3].msg)

    def test_queue_connect_amqpok(self):
        result = self.queue_publisher._connect()
        self.assertEqual(result, True)

    def test_queue_disconnect_ok(self):
        self.queue_publisher._connect()

        with self.assertLogs(level='DEBUG') as cm:
            self.queue_publisher._disconnect()

        msg = 'Disconnected from rabbit'
        self.assertIn(msg, cm[1][-1])

    def test_exchange_connect_amqpok(self):
        result = self.exchange_publisher._connect()
        self.assertEqual(result, True)

    def test_exchange_disconnect_ok(self):
        self.exchange_publisher._connect()

        with self.assertLogs(level='DEBUG') as cm:
            self.exchange_publisher._disconnect()

        msg = 'Disconnected from rabbit'
        self.assertIn(msg, cm[1][-1])

    def test_durable_exchange_connect_amqpok(self):
        result = self.durable_exchange_publisher._connect()
        self.assertEqual(result, True)

    def test_durable_exchange_disconnect_ok(self):
        self.durable_exchange_publisher._connect()

        with self.assertLogs(level='DEBUG') as cm:
            self.durable_exchange_publisher._disconnect()

        msg = 'Disconnected from rabbit'
        self.assertIn(msg, cm[1][-1])

    def test_queue_disconnect_already_closed_connection(self):
        self.queue_publisher._connect()
        self.queue_publisher._disconnect()
        with self.assertLogs(level='DEBUG') as cm:
            self.queue_publisher._disconnect()

        msg = 'Close called on closed connection'
        self.assertIn(msg, cm.output[0])

    def test_exchange_disconnect_already_closed_connection(self):
        self.exchange_publisher._connect()
        self.exchange_publisher._disconnect()
        with self.assertLogs(level='DEBUG') as cm:
            self.exchange_publisher._disconnect()

        msg = 'Close called on closed connection'
        self.assertIn(msg, cm.output[0])

    def test_durable_exchange_disconnect_already_closed_connection(self):
        self.durable_exchange_publisher._connect()
        self.durable_exchange_publisher._disconnect()
        with self.assertLogs(level='DEBUG') as cm:
            self.durable_exchange_publisher._disconnect()

        msg = 'Close called on closed connection'
        self.assertIn(msg, cm.output[0])

    def test_queue_publish_message_no_connection(self):
        with self.assertRaises(PublishMessageError):
            self.bad_queue_publisher.publish_message(test_data['valid'])

    def test_queue_publish(self):
        self.queue_publisher._connect()
        with self.assertLogs(level='INFO') as cm:
            result = self.queue_publisher.publish_message(test_data['valid'])
            self.assertEqual(True, result)
        self.assertIn('Published message to queue', cm.output[3])

    def test_queue_publish_nack_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = NackError('a')
            self.queue_publisher._connect()
            with self.assertRaises(PublishMessageError):
                self.queue_publisher.publish_message(test_data['valid'])

    def test_queue_publish_unroutable_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = UnroutableError('a')
            self.queue_publisher._connect()
            with self.assertRaises(PublishMessageError):
                self.queue_publisher.publish_message(test_data['valid'])

    def test_queue_publish_generic_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = Exception()
            self.queue_publisher._connect()
            with self.assertRaises(Exception):
                self.queue_publisher.publish_message(test_data['valid'])

    def test_exchange_publish_message_no_connection(self):
        with self.assertRaises(PublishMessageError):
            self.bad_exchange_publisher.publish_message(test_data['valid'])

    def test_exchange_publish(self):
        self.exchange_publisher._connect()
        with self.assertLogs(level='INFO') as cm:
            result = self.exchange_publisher.publish_message(test_data['valid'])
            self.assertEqual(True, result)
        self.assertIn('Published message to exchange', cm.output[3])

    def test_exchange_publish_nack_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = NackError('a')
            self.exchange_publisher._connect()
            with self.assertRaises(PublishMessageError):
                self.exchange_publisher.publish_message(test_data['valid'])

    def test_exchange_publish_unroutable_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = UnroutableError('a')
            self.exchange_publisher._connect()
            with self.assertRaises(PublishMessageError):
                self.exchange_publisher.publish_message(test_data['valid'])

    def test_exchange_publish_generic_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = Exception()
            self.exchange_publisher._connect()
            with self.assertRaises(Exception):
                self.exchange_publisher.publish_message(test_data['valid'])

    def test_durable_exchange_publish_message_no_connection(self):
        with self.assertRaises(PublishMessageError):
            self.bad_durable_exchange_publisher.publish_message(test_data['valid'])

    def test_durable_exchange_publish(self):
        self.durable_exchange_publisher._connect()
        with self.assertLogs(level='INFO') as cm:
            result = self.durable_exchange_publisher.publish_message(test_data['valid'])
            self.assertEqual(True, result)
        self.assertIn('Published message to exchange', cm.output[3])

    def test_durable_exchange_publish_nack_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = NackError('a')
            self.durable_exchange_publisher._connect()
            with self.assertRaises(PublishMessageError):
                self.durable_exchange_publisher.publish_message(test_data['valid'])

    def test_durable_exchange_publish_unroutable_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = UnroutableError('a')
            self.durable_exchange_publisher._connect()
            with self.assertRaises(PublishMessageError):
                self.durable_exchange_publisher.publish_message(test_data['valid'])

    def test_durable_exchange_publish_generic_error(self):
        mock_method = 'pika.adapters.blocking_connection.BlockingChannel.basic_publish'
        with mock.patch(mock_method) as barMock:
            barMock.side_effect = Exception()
            self.durable_exchange_publisher._connect()
            with self.assertRaises(Exception):
                self.durable_exchange_publisher.publish_message(test_data['valid'])
