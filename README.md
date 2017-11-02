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

## License

Open source licensed under the MIT license (see _LICENSE_ file for details).