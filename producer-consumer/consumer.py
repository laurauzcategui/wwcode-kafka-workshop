#!/usr/bin/env python3

# import the KafkaConsumer
from kafka import KafkaConsumer
import argparse
# import kafka_utils module
import kafka_utils
import io
import avro.io
from avro.io import DatumReader

# change this to point to your new schema :)
SCHEMA_PATH = '../avro-schemas/user.avsc'

# SCHEMA
SCHEMA = kafka_utils.read_schema(SCHEMA_PATH)

def consume(consumer_id, topic):
    print("Started consuming from consumer-{}".format(consumer_id))

    # Create a Consumer below this area :)
    consumer = KafkaConsumer(group_id='ww-group',
                             bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             consumer_timeout_ms=20000)

    # subscribe to the topic you would like to consume from
    consumer.subscribe(topic)

    for message in consumer:
        # create a in-memory bytes buffer
        bytes_reader = io.BytesIO(message.value)
        # create the binary Decoder
        decoder = avro.io.BinaryDecoder(bytes_reader)
        # create a datumReader responsible to deserialize the data from avro to readable format using the Schema
        reader = avro.io.DatumReader(SCHEMA)
        # Read the incoming data
        value = reader.read(decoder)
        print ("Consumer-{} Topic:{}, Partition:{} Offset:{}: key={} value={}".format(consumer_id,
              message.topic,
              message.partition,
              message.offset,
              message.key,
              value)
              )

    consumer.close()

def help_msg():
    return "e.g.: python consumer.py --consumer-id 1"

def args_parser():
    parser = argparse.ArgumentParser(epilog=help_msg())
    parser.add_argument('-c','--consumer-id', default=1, help='Consumer-ID, will identify from where you are consuming from', required=True, type=int, dest='consumer')
    parser.add_argument('--topic', help='Topic on which to write the data', required=True)
    args = parser.parse_args()
    return args

def main():
    args = args_parser()
    print(args)
    consume(args.consumer, args.topic)

if __name__ == "__main__":
    main()
