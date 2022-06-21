import pika
import json
import os.path
from flask import Flask
from typing import Callable

from neon_utils import LOG
from neon_mq_connector.connector import MQConnector
from neon_mq_connector.utils.rabbit_utils import create_mq_callback


class LibreMQ(MQConnector):
    """
    Module for processing MQ requests from PyKlatchat to LibreTranslate"""

    def __init__(self, app: Flask, translate_request: Callable):
        config = self.load_mq_config()
        super().__init__(config = config, service_name = 'mq-libre-translate')

        self.app = app
        self.translate_request = translate_request

        self.vhost = '/translation'
        self.register_consumer(name='request_libre_translations',
                               vhost=self.vhost,
                               queue='request_libre_translations',
                               callback=self.handle_translate_request,
                               on_error=self.default_error_handler,
                               auto_ack=False)

    def load_mq_config(self, config_path: str = "app/configs/config.json"):
        default_config_path = "app/configs/default_config.json"

        config_path = config_path if os.path.isfile(config_path) else default_config_path
        with open(config_path) as config_file:
            config = json.load(config_file)
        LOG.info(f"Loaded MQ config from path {config_path}")
        return config

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