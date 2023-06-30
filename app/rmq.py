import pika
import json
import os.path
from flask import Flask
from typing import Callable

from ovos_utils.log import LOG
from neon_mq_connector.connector import MQConnector
from neon_mq_connector.utils.rabbit_utils import create_mq_callback
from ovos_config.config import Configuration


class LibreMQ(MQConnector):
    """
    Module for processing MQ requests from PyKlatchat to LibreTranslate"""

    def __init__(self, app: Flask, translate_request: Callable):
        config = self.load_mq_config()
        super().__init__(config=config, service_name='mq-libre-translate')

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
        if config_path is os.path.isfile(config_path):
            LOG.warning(f"Legacy configuration found at: {config_path}")
            with open(config_path) as config_file:
                config = json.load(config_file)
            return config

        config = Configuration()
        if not config.get("MQ"):
            LOG.warning("No MQ config found, using default")
            with open(default_config_path) as config_file:
                config = json.load(config_file)
        LOG.info(f"Loaded MQ config")
        return config.get("MQ", config)

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
        request_id = body["request_id"]
        pyklatchat_data = body["data"]

        with self.app.app_context():
            translation_json = self.translate_request(pyklatchat_data)
            translation = translation_json.json

        translation_response = {
            "request_id": request_id,
            "data": translation
        }
        self.send_message(translation_response, queue = 'get_libre_translations')
        channel.basic_ack(method.delivery_tag)
