package flume

const errorsSchema = `
{
  "type": [
    "string"
  ]
}
`

const eventSchema = `
{
  "type": "record",
  "name": "AvroFlumeEvent",
  "fields": [
    {
      "name": "headers",
      "type": {
        "type": "map",
        "values": "string"
      }
    },
    {
      "name": "body",
      "type": "bytes"
    }
  ]
}
`

const eventsSchema = `
{
  "type": "array",
  "items": {
    "type": "record",
    "name": "AvroFlumeEvent",
    "fields": [
      {
        "name": "headers",
        "type": {
          "type": "map",
          "values": "string"
        }
      },
      {
        "name": "body",
        "type": "bytes"
      }
    ]
  }
}
`

const statusSchema = `
{
  "type": "enum",
  "name": "Status",
  "symbols": [
    "OK",
    "FAILED",
    "UNKNOWN"
  ]
}
`

const messageProtocol = `
{
  "protocol": "AvroSourceProtocol",
  "namespace": "org.apache.flume.source.avro",
  "doc": "* Licensed to the Apache Software Foundation (ASF) under one\n * or more contributor license agreements.  See the NOTICE file\n * distributed with this work for additional information\n * regarding copyright ownership.  The ASF licenses this file\n * to you under the Apache License, Version 2.0 (the\n * \"License\"); you may not use this file except in compliance\n * with the License.  You may obtain a copy of the License at\n *\n * http://www.apache.org/licenses/LICENSE-2.0\n *\n * Unless required by applicable law or agreed to in writing,\n * software distributed under the License is distributed on an\n * \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n * KIND, either express or implied.  See the License for the\n * specific language governing permissions and limitations\n * under the License.",
  "types": [
    {
      "type": "enum",
      "name": "Status",
      "symbols": [
        "OK",
        "FAILED",
        "UNKNOWN"
      ]
    },
    {
      "type": "record",
      "name": "AvroFlumeEvent",
      "fields": [
        {
          "name": "headers",
          "type": {
            "type": "map",
            "values": "string"
          }
        },
        {
          "name": "body",
          "type": "bytes"
        }
      ]
    }
  ],
  "messages": {
    "append": {
      "request": [
        {
          "name": "event",
          "type": "AvroFlumeEvent"
        }
      ],
      "response": "Status"
    },
    "appendBatch": {
      "request": [
        {
          "name": "events",
          "type": {
            "type": "array",
            "items": "AvroFlumeEvent"
          }
        }
      ],
      "response": "Status"
    }
  }
}
`
