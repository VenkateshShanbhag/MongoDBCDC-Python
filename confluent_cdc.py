import json

import pymongo
import datetime
import ccloud_lib
import configparser
from confluent_kafka import Producer


def run_app():
    config = configparser.ConfigParser()
    config.read('conf.env')

    args = ccloud_lib.parse_args()
    config_file = args.config_file
    conf = ccloud_lib.read_ccloud_config(config_file)
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    client = pymongo.MongoClient(config.get('MDB', 'DB_HOST'), tlsAllowInvalidCertificates=True)
    mydb = client[config.get('MDB', 'DB_DATABASE')]
    change_stream = mydb.get_collection(config.get('MDB', 'DB_COLLECTION')).watch(full_document="updateLookup")
    print("change stream started")
    for change in change_stream:
        # print(change)
        date = datetime.datetime.now()

        data = {"timestamp": str(date), "source_data": json.dumps(change["fullDocument"], default=str),
                "_id": str(change["fullDocument"]["_id"])}
        print(data)

        producer.produce("test", key="timeseries data", value=json.dumps(data), on_delivery=ack)


def ack(err, msg):
    global delivered_records
    """
    Message Delivered Successfully @!!!!!!
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
              .format(msg.topic(), msg.partition(), msg.offset()))


run_app()
