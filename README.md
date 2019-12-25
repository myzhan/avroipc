[![Build Status](https://github.com/myzhan/avroipc/workflows/Go/badge.svg)](https://github.com/myzhan/avroipc/actions?workflow=Go)
[![Coverage Status](https://coveralls.io/repos/github/myzhan/avroipc/badge.svg?branch=master)](https://coveralls.io/github/myzhan/avroipc?branch=master)

# avroipc

Avroipc is a pure-go-implemented client for [flume's avro source](http://flume.apache.org/FlumeUserGuide.html#avro-source).

I wrote avroipc to learn avro rpc protocol, and it's **not production ready!**, use it as you own risk.

Thanks to Linkedin's [goavro](https://github.com/linkedin/goavro)!

## Usage

```go
package main

import (
    "log"

    "github.com/myzhan/avroipc"
)

func main() {
    // flume avro instance address
    client, err := avroipc.NewClient("localhost:20200", 0, 0, 1024, 6)
    if err != nil {
        log.Fatal(err)
    }
    
    event := &avroipc.Event{
        Body: []byte("hello from go"),
    	Headers: map[string]string {
            "topic": "myzhan",
            "timestamp": "1508740315478",
        },
    }
    status, err := client.Append(event)
    if err != nil {
        log.Fatal(err)
    }
    if status != "OK" {
        log.Fatalf("Bad status: %s", status)
    }
}
```

## Development

Clone the repository and do the following sequence of command:
```bash
go get
go test ./...
```

To run a test with a real client run the following command:
```bash
FLUME_SERVER_ADDRESS=127.0.0.1:20201 go test -count=1 -run TestSend
```
where `127.0.0.1:20201` is a real Apache Flume server, `-count=1` is a way to disable Go build cache.

If you want to run a test with a real client and enabled data compression run the following command:
```bash
FLUME_SERVER_ADDRESS=127.0.0.1:20201 FLUME_COMPRESSION_LEVEL=1 go test -count=1 -run TestSend
```
where `FLUME_COMPRESSION_LEVEL` is a new environment variable to specify wanted compression level.
Support values from `1` to `9`.

## License

Open source licensed under the MIT license (see _LICENSE_ file for details).
