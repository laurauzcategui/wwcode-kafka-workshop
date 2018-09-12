#!/usr/bin/env python3

import json
import io
import argparse
import time

from sys import argv
import sys

# Import the avro library
import avro.io
from avro.io import DatumWriter

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
    pass

if __name__ == '__main__':
    args = args_parser()
    produce_messages(args.broker, args.topic, args.schema_name, args.data_path)
