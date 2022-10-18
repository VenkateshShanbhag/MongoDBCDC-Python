import pymongo
# from flask import Flask
from google.cloud import pubsub_v1
import datetime

#
# app = Flask(__name__)
#
#
# @app.route("/")
# def hello_world():
#     name = "Venkatesh"
#     return "Hello {}!".format(name)


def run_app():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path("gsidemo-246315", "Test")

    client = pymongo.MongoClient("mongodb+srv://venkatesh:ashwin123@iiotapp.2wqno.mongodb.net",
                                 tlsAllowInvalidCertificates=True)
    mydb = client["sample_mflix"]
    change_stream = mydb.get_collection("movies").watch(full_document="updateLookup")
    print("change stream started")
    for change in change_stream:
        # print(change)
        date = datetime.datetime.now()

        data = {"timestamp": str(date), "source_data": change["fullDocument"], "_id": change["fullDocument"]["_id"]}
        print(data)
        future = publisher.publish(topic_path, str(data).encode("utf-8"))
        print(future.result())
        print(f"Published messages to {topic_path}.")


if __name__ == "__main__":
    run_app()
    # app.run(debug=True, host="0.0.0.0", port=5001)
