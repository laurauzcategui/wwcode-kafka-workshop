#!/usr/bin/env python3
import json
import io
import argparse

from sys import argv
import sys

# Import the avro library
import avro.io
from avro.io import DatumWriter

from kafka import KafkaProducer
import kafka_utils

def help_msg():
    return "e.g.: python producer.py --schema_path ../avro-schemas/user.avsc --data_file ../data/input.data --broker=kafka --topic=test"

def args_parser():
    parser = argparse.ArgumentParser(
        description='Parse a file of data and its AVRO description to then inject it in a kafka topic.',
        epilog=help_msg())
    parser.add_argument('--schema_name', default='../avro-schemas/user.avsc',help='Path to file containing the .avsc schema file')
    parser.add_argument('--data_path', default='../data/wwcode.data', help='Path to the file containing the data')
    parser.add_argument('--broker',    help='Broker where to send the data', required=True)
    parser.add_argument('--topic',     help='Topic on which to write the data', required=True)

    args = parser.parse_args()
    return args

def produce_messages(broker,topic,schema_name,data):
    '''
    It will receive the args and produce messages by sending to the broker and
    '''

    schema = kafka_utils.read_schema(schema_name)

    # get a writter responsible to serialize data to avro format
    writer =  avro.io.DatumWriter(schema)

    '''
        Let's add our code to start producing messages HERE :)
    '''
    producer = KafkaProducer( bootstrap_servers='{}:9092'.format(broker),
                              client_id = "AwesomeKafkaProducer",
                              acks=1,
                              retries=3)
    config = producer.config
    print("Created kafka producer client_id: {}, broker:{}, acks:{} and retries:{} "
        .format(config['client_id'], config['bootstrap_servers'], config['acks'], config['retries']))


    msgs_sent_counter = 0

    with open(data) as f:
        for line in f:
            if not line.strip():
                continue
            # load each line of your data file
            msg = json.loads(line)

            # get an in-memory byte buffer
            bytes_writer = io.BytesIO()

            # get the avro binary encoder
            encoder = avro.io.BinaryEncoder(bytes_writer)

            # serialize the data with the message and the encoder
            writer.write(msg, encoder)

            # get the serialized bytes from the buffer
            raw_bytes = bytes_writer.getvalue()

            # Send messages to the topic :)
            producer.send(topic, value=raw_bytes)
            msgs_sent_counter += 1
            # Uncomment the line below if you want to see the message you are sending
            print("Sending message: {}".format(line))

        print("Sent {} messages".format(msgs_sent_counter))
    producer.close()

if __name__ == '__main__':
    args = args_parser()
    produce_messages(args.broker, args.topic, args.schema_name, args.data_path)
