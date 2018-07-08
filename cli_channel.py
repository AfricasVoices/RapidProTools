import argparse

from flask import Flask, request
from threading import Thread
from flask_restful import Resource, Api
import requests
import time
import logging

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RapidPro Command Line Channel")
    parser.add_argument("-p", "--port", help="Port to run on", default=8082)
    parser.add_argument("server", help="Base URL of server channel to connect to, including c/ex/<uuid>/", nargs=1)
    parser.add_argument("phone", metavar="phone-number", help="Phone number to send messages from", nargs=1)

    args = parser.parse_args()
    port = args.port
    server = args.server[0]
    phone_number = args.phone[0]


    class Messages(Resource):
        def post(self):
            text = request.args.get("text")
            id = request.args.get("id")
            print(text)
            requests.post(server + "delivered", params={"id": id})

        @staticmethod
        def send_message(message):
            requests.post(server + "receive?from=" + phone_number + "&text=" + message)


    app = Flask(__name__)
    api = Api(app)
    api.add_resource(Messages, "/messages")


    def input_loop():
        time.sleep(5)  # Give the server enough time to start. TODO: Implement less yuckily.
        while True:
            response = input("> ")
            Messages.send_message(response)


    thread = Thread(target=input_loop)
    thread.start()

    app.run(port=port)
