package avroipc

var handshakeRequestProtocol = `
{
  "type": "record",
  "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
  "fields": [
    {"name": "clientHash",
     "type": {"type": "fixed", "name": "MD5", "size": 16}},
    {"name": "clientProtocol", "type": ["null", "string"]},
    {"name": "serverHash", "type": "MD5"},
    {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
  ]
}
`

var handshakeResponseProtocol = `
{
  "type": "record",
  "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc",
  "fields": [
    {"name": "match",
     "type": {"type": "enum", "name": "HandshakeMatch",
              "symbols": ["BOTH", "CLIENT", "NONE"]}},
    {"name": "serverProtocol",
     "type": ["null", "string"]},
    {"name": "serverHash",
     "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
    {"name": "meta",
     "type": ["null", {"type": "map", "values": "bytes"}]}
  ]
}
`

var messageProtocol = `
{
    "protocol": "AvroSourceProtocol",
    "namespace": "org.apache.flume.source.avro",
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

var eventProtocol = `
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

var metaProtocol = `
{"type": "map", "values": "bytes"}
`
