#!/usr/bin/env python3

# import the KafkaConsumer
from kafka import KafkaConsumer

import argparse

def consume(consumer_id, topic):
    print("Started consuming from consumer-{}".format(consumer_id))

    # Create a Consumer below this area :)
    pass
    

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
