[![Build Status](https://github.com/myzhan/avroipc/workflows/Go/badge.svg)](https://github.com/myzhan/avroipc/actions?workflow=Go)
[![Coverage Status](https://coveralls.io/repos/github/myzhan/avroipc/badge.svg?branch=master)](https://coveralls.io/github/myzhan/avroipc?branch=master)

# avroipc

Avroipc is a pure-go-implemented client for [flume's avro source](http://flume.apache.org/FlumeUserGuide.html#avro-source).

I wrote avroipc to learn avro rpc protocol, and it's **not production ready!**, use it as you own risk.

Thanks to Linkedin's [goavro](https://github.com/linkedin/goavro)!

## Usage

```go
// flume avro instance address
client := NewClient("localhost:20200")

headersMap := make(map[string]string)
headersMap["topic"] = "myzhan"
headersMap["timestamp"] = "1508740315478"
body := []byte("hello from go")

event := NewEvent(headersMap, body)
client.Append(event)
```

## Development

Clone the repository and do the following sequence of command:
```bash
go get
go test ./...
```

To run test with a real client run the following command:
```bash
FLUME_SERVER_ADDRESS=127.0.0.1:20201 go test -count=1 -run TestSend
```
where `127.0.0.1:20201` is a real Apache Flume server, `-count=1` is a way to disable Go build cache.

## License

Open source licensed under the MIT license (see _LICENSE_ file for details).
