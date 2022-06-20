import pika
from flask import Flask
from typing import Callable

from neon_mq_connector.connector import MQConnector
from neon_mq_connector.utils.rabbit_utils import create_mq_callback


class LibreMQ(MQConnector):
    """
    Module for processing MQ requests from PyKlatchat to LibreTranslate"""

    def __init__(self, app: Flask, translate_request: Callable):
        super().__init__(config = {"MQ": {'users':{'mq-libre-translate':{}}}}, 
                         service_name = 'mq-libre-translate')

        self.app = app
        self.translate_request = translate_request

        self.vhost = '/translation'
        self.register_consumer(name='request_libre_translations',
                               vhost=self.vhost,
                               queue='request_libre_translations',
                               callback=self.handle_translate_request,
                               on_error=self.default_error_handler,
                               auto_ack=False)

    def send_responce(self, message: dict,
                            routing_key: str):
        """
        Sends responses to PyKlatchat

        :param message: Message dict
        :param routing_key: Queue to post response to
        """

        with self.create_mq_connection(vhost=self.vhost) as mq_connection:
            self.emit_mq_message(connection=mq_connection,
                                 request_data=message,
                                 queue=routing_key)

    @create_mq_callback(include_callback_props=('channel', 'method', 'body'))
    def handle_translate_request(self,
                            channel: pika.channel.Channel,
                            method: pika.spec.Basic.Return,
                            body: dict):
        """
        Handles translation requests from MQ to LibreTranslate received on queue
        "request_libre_translations"

        :param channel: MQ channel object (pika.channel.Channel)
        :param method: MQ return method (pika.spec.Basic.Return)
        :param body: request body (dict)

        """
        body.pop("message_id", None)

        with self.app.app_context():
            translation_json = self.translate_request(body)
            translation = translation_json.json

        self.send_message(translation, queue = 'get_libre_translations')
        channel.basic_ack(method.delivery_tag)