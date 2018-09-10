import json
import io
import argparse
import time

from sys import argv
import sys

# Import the avro library
import avro.schema
from avro.io import DatumWriter

from kafka import SimpleProducer, KafkaClient

def args_parser():
    parser = argparse.ArgumentParser(
        description='Parse a file of data and its AVRO description to then inject it in a kafka topic.',
        epilog="e.g.: python producer.py --schema_path ../avro-schemas/user.avsc --data_file ../data/input.data --broker=kafka --topic=test")
    parser.add_argument('--schema_name', default='../avro-schemas/user.avsc',help='Path to file containing the .avsc schema file')
    parser.add_argument('--data_path', default='../data/wwcode.data', help='Path to the file containing the data')
    parser.add_argument('--broker',    help='Broker where to send the data')
    parser.add_argument('--topic',     help='Topic on which to write the data')

    args = parser.parse_args()
    validate_arguments(args)
    return args

def validate_arguments(args):
    '''
    It will receive the args and validate that broker and topic are being passed
    as parameters
    '''
    if args.broker is None:
        sys.exit('Broker needs to be specified :)')
    if args.topic is None:
        sys.exit('Please specify the topic to send your messages to')

def read_schema(schema_path):
    '''
    It will read the avro schema file and return the parsed schema
    '''
    with open(schema_path) as f:
            schema = avro.schema.Parse(f.read())
    return schema

def produce_messages(broker,topic,schema_name,data):
    '''
    It will receive the args and produce messages by sending to the broker and
    '''

    schema = read_schema(schema_name)

    # get a writter responsible to serialize data to avro format
    writer =  avro.io.DatumWriter(schema)

    '''
        Let's add our code to start producing messages HERE :)
    '''
    pass

if __name__ == '__main__':
    args = args_parser()
    produce_messages(args.broker, args.topic, args.schema_name, args.data_path)
