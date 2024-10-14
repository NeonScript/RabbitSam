import argparse
import json
import os

import pika


class RabbitMQ:
    def __init__(self, queue_name: str) -> None:
        """
        constructor for RabbitMq class.\n
        in this constructor we create Cli arguments for connect to RabbitMQ.
        """
        self.host = None
        self.port = None
        self.username = None
        self.password = None
        self.queue_name = queue_name

        # section Creating Cli arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("-H", "--host", help="rabbitmq host")
        parser.add_argument("-p", "--port", help="rabbitmq port")
        parser.add_argument("-U", "--username", help="rabbitmq username")
        parser.add_argument("-P", "--password", help="rabbitmq password")

        self.args = parser.parse_args()
        # !section

    def _set_host(self, rabbit_host_env_var: str) -> None:
        """Set the RabbitMQ host."""
        host = os.getenv(rabbit_host_env_var)
        if host is None:
            if not self.args.host:
                raise ValueError(
                    "The host is None. Please set the host using the --host flag."
                )
            host = self.args.host
        self.host = host

    def _set_port(self, rabbit_port_env_var: str) -> None:
        """Set the RabbitMQ port."""
        port = os.getenv(rabbit_port_env_var)
        if port is None:
            if self.args.port is None:
                raise ValueError(
                    "The port is None. Please set the port using the --port flag."
                )
            port = self.args.port
        self.port = int(port)

    def _set_username(self, rabbit_username_env_var: str) -> None:
        """Set the RabbitMQ username."""
        username = os.getenv(rabbit_username_env_var)
        if username is None:
            if self.args.username is None:
                raise ValueError(
                    "The username is None. Please set the username using the --username flag."
                )
            username = self.args.username
        self.username = username

    def _set_password(self, rabbit_password_env_var: str) -> None:
        """Set RabbitMQ password."""
        password = os.getenv(rabbit_password_env_var)
        if password is None:
            if not self.args.password:
                raise ValueError(
                    "The password is None. Please set the password using the --password flag."
                )
            password = self.args.password
        self.password = password

    def connect(
        self,
        host_env_var: str = "RABBITMQ_HOST",
        port_env_var: str = "RABBITMQ_PORT",
        username_env_var: str = "RABBITMQ_USERNAME",
        password_env_var: str = "RABBITMQ_PASSWORD",
    ) -> None:
        """
        Connect to RabbitMQ.

        Parameters
        ----------
        host_env_var : str
            Environment variable name for RabbitMQ host.
        port_env_var : str
            Environment variable name for RabbitMQ port.
        username_env_var : str
            Environment variable name for RabbitMQ username.
        password_env_var : str
            Environment variable name for RabbitMQ password.
        """
        self._set_host(host_env_var)
        self._set_port(port_env_var)
        self._set_username(username_env_var)
        self._set_password(password_env_var)

        if not self.host:
            raise ValueError(
                "The host is None. Please set the host using the --host flag."
            )
        if not self.port:
            raise ValueError(
                "The port is None. Please set the port using the --port flag."
            )
        if not self.username:
            raise ValueError(
                "The username is None. Please set the username using the --username flag."
            )
        if not self.password:
            raise ValueError(
                "The password is None. Please set the password using the --password flag."
            )

        connection_params = pika.ConnectionParameters(
            self.host,
            self.port,
            credentials=pika.PlainCredentials(
                username=self.username, password=self.password
            ),
        )

        self.connection = pika.BlockingConnection(connection_params)

        if not self.connection:
            raise ValueError(
                "The connection is None. Please call rabbit_connect first."
            )

        self.channel = self.connection.channel()

        if not self.channel:
            raise ValueError("The channel is None. Please call rabbit_connect first.")

    def queue_declare(self, queue_name: str, durable: bool = True) -> None:
        """
        Declare a queue.

        :param queue_name: Name of the RabbitMQ queue.
        :param durable: Whether or not the queue should be durable.
        """
        if not self.channel:
            raise ValueError("The channel is None. Please call rabbit_connect first.")
        self.channel.queue_declare(queue=queue_name, durable=durable)

    def queue_bind(self, exchange_name: str, queue_name: str) -> None:
        """
        Bind a queue to an exchange.

        :param exchange_name: Name of the RabbitMQ exchange.
        :param queue_name: Name of the RabbitMQ queue.
        """
        if not self.channel:
            raise ValueError("The channel is None. Please call rabbit_connect first.")
        if not exchange_name:
            raise ValueError("Exchange name is required.")
        if not queue_name:
            raise ValueError("Queue name is required.")

        self.channel.queue_bind(exchange=exchange_name, queue=queue_name)

    def consume_queue(self, callback_func: callable) -> None:
        """
        Consume a RabbitMQ queue.

        :param callback_func: Callback function.
        """
        if not self.channel:
            raise ValueError("Channel is not established. Call rabbit_connect first.")
        if not callback_func:
            raise ValueError("Callback function is required.")

        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=callback_func, auto_ack=True
        )
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
        finally:
            self.channel.close()

    def exchange_declare(
        self, exchange_name: str, exchange_type: str = "direct"
    ) -> None:
        """
        Declare an exchange.

        :param exchange_name: Name of the RabbitMQ exchange.
        :param exchange_type: Type of the RabbitMQ exchange. Default is direct.
        """
        if not self.channel:
            raise ValueError("The channel is None. Please call rabbit_connect first.")
        if not exchange_name:
            raise ValueError("Exchange name is required.")

        self.channel.exchange_declare(
            exchange=exchange_name, exchange_type=exchange_type
        )

    def publish_message(
        self, exchange_name: str, routing_key: str, message: dict
    ) -> None:
        self.channel.basic_publish(
            exchange=exchange_name, routing_key=routing_key, body=json.dumps(message)
        )
