from flask import Flask, request
from threading import Thread
from flask_restful import Resource, Api
import requests
import time
import logging

START_PHONE_NUMBER = "11111000000"  # Number to start the first flow from. Will increment by 1 for each subsequent run.
SERVER_ADDR = "http://localhost:8000/c/ex/e9148e8f-d230-4fe3-8705-bb1ee760b10f/"
PORT = 8082

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


class Messages(Resource):
    def post(self):
        received_message = request.args.get("text")
        source = request.args.get("to")

        if received_message == "Do you prefer llamas or alpacas?":
            Messages.send_message("llamas", source)
        elif received_message == "What is your gender?":
            Messages.send_message("m", source)
        elif received_message == "How old are you?":
            Messages.send_message("35", source)
        elif received_message == "Thanks for your responses!":
            print("Completed " + str(source))
            Messages.send_message("camelid", int(source[1:]) + 1)
        else:
            pass
            print("Unknown message received: " + received_message)
        return

    @staticmethod
    def send_message(message, phone_from):
        inner_thread = Thread(target=requests.post,
                              args=(SERVER_ADDR + "/receive?from=" + str(phone_from) + "&text=" + message,))
        inner_thread.start()


app = Flask(__name__)
api = Api(app)
api.add_resource(Messages, "/messages")


def trigger():
    time.sleep(5)
    print("Triggering first run")
    Messages.send_message("camelid", START_PHONE_NUMBER)


def run_server():
    app.run(port=PORT)


if __name__ == "__main__":
    thread = Thread(target=trigger)
    thread.start()

    run_server()
