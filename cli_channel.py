from flask import Flask, request
from threading import Thread
from flask_restful import Resource, Api
import requests
import time
import logging

PHONE_NUMBER = "441632300010"
SERVER_ADDR = ""
PORT = 8082

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


class Messages(Resource):
    def post(self):
        text = request.args.get("text")
        print(text)

        # Handle the user's response on a new thread so that POSTing doesn't timeout.
        thread = Thread(target=self.collect_next_message)
        thread.start()

    @classmethod
    def collect_next_message(cls):
        response = raw_input("> ")
        cls.send_message(response)

    @staticmethod
    def send_message(message):
        requests.post(SERVER_ADDR + "receive?from=" + PHONE_NUMBER + "&text=" + message)


app = Flask(__name__)
api = Api(app)
api.add_resource(Messages, "/messages")


def collect_first_message():
    time.sleep(5)  # Give the server enough time to start. TODO: Implement less yuckily.
    Messages.collect_next_message()


def run_server():
    app.run(port=PORT)


if __name__ == "__main__":
    thread = Thread(target=collect_first_message)
    thread.start()

    run_server()
