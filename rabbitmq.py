import argparse
import json
import os

import pika


class RabbitMQ:
    def __init__(self) -> None:
        """
        constructor for RabbitMq class.\n
        in this constructor we create Cli arguments for connect to RabbitMQ.
        """
        self.host = "localhost"
        self.port = 5672
        self.username = "guest"
        self.password = "guest"
        self.queue_name = "hello"

        # section Creating Cli arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("-H", "--host", help="rabbitmq host")
        parser.add_argument("-p", "--port", help="rabbitmq port")
        parser.add_argument("-U", "--username", help="rabbitmq username")
        parser.add_argument("-P", "--password", help="rabbitmq password")

        self.args = parser.parse_args()
        # !section

    def _set_host(self, RABBIT_HOST: str) -> None:
        """Private method for setting rabbitmq host."""
        self.host = os.getenv(RABBIT_HOST) or self.args.host or self.host

    def _set_port(self, RABBIT_PORT: str) -> None:
        """Private method for setting rabbitmq port."""
        self.port = int(os.getenv(RABBIT_PORT, self.args.port or self.port))

    def _set_username(self, RABBIT_USERNAME: str) -> None:
        """Set rabbitmq username."""
        self.username = os.getenv(RABBIT_USERNAME) or self.args.username

    def _set_password(self, RABBIT_PASSWORD: str) -> None:
        """Set rabbitmq password."""
        self.password = os.getenv(RABBIT_PASSWORD) or self.args.password

    def rabbit_connect(
        self,
        rabbit_host: str = "RABBIT_HOST",
        rabbit_port: str = "RABBIT_PORT",
        rabbit_username: str = "RABBIT_USERNAME",
        rabbit_password: str = "RABBIT_PASSWORD",
    ) -> None:
        """
        method for connecting to RabbitMQ.
        Params:
            rabbit_host (str): rabbitmq host ENV variable name.
            rabbit_port (str): rabbitmq port ENV variable name.
            rabbit_username (str): rabbitmq username ENV variable name.
            rabbit_password (str): rabbitmq password ENV variable name.
        """
        self._set_host(rabbit_host)
        self._set_port(rabbit_port)
        self._set_username(rabbit_username)
        self._set_password(rabbit_password)

        connection_params = pika.ConnectionParameters(
            self.host,
            self.port,
            credentials=pika.PlainCredentials(self.username, self.password),
        )
        self.connection = pika.BlockingConnection(connection_params)
        self.channel = self.connection.channel()

    def exchange_declare(self, exchange_name: str, exchange_type: str) -> None:
        """
        method for creating a rabbitmq exchange.
        Params:
            exchange_name (str): name of the rabbitmq exchange.
        """
        self.channel.exchange_declare(
            exchange=exchange_name, exchange_type=exchange_type
        )

    def producer_publish_message(
        self, exchange_name: str, routing_key: str, message: str
    ) -> None:
        """
        method for sending a message to a queue.
        Params:
            message (str): message to send.
            routing_key (str): routing key.
            exchange_name (str): exchange name.
        """
        data_body = json.dumps(message)

        self.channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=data_body,
        )

    def consumer_bind_queue(
        self, exchange_name: str, queue_name: str, routing_key: str
    ) -> None:
        """
        method for binding a queue to an exchange.
        Params:
            exchange_name (str): name of the rabbitmq exchange.
            queue_name (str): name of the rabbitmq queue.
            routing_key (str): routing key.
        """
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.queue_bind(
            exchange=exchange_name,
            queue=queue_name,
            routing_key=routing_key,
            arguments={"x-queue-mode": "lazy"},
        )

    def consumer_consume_message(self, cb: callable) -> None:
        """
        method for consuming a queue.
        Params:
            cb (callable): callback function.
        """
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=cb)
        self.channel.start_consuming()
