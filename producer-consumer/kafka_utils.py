#!/usr/bin/env python3

import avro.schema

def read_schema(schema_path):
    '''
    It will read the avro schema file and return the parsed schema
    '''
    with open(schema_path) as f:
            schema = avro.schema.Parse(f.read())
    return schema
