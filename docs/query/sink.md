Sink receives events from a stream and send them to the relative receiver. Currently Wisdom supports the following sinks:

- HTTP
- Kafka
- Text File
- Console

**Java API:**

Define HTTP sink in Java.

```java
wisdomApp.defineQuery("query1")
        .from("StockStream")
        .select("symbol", "price")
        .insertInto("OutputStream");

wisdomApp.addSink("OutputStream", new HTTPSink("http://localhost:9999/streamReceiver"));
```

**Wisdom Query:**

Define HTTP sink in Wisdom Query.

```java
def stream StockStream;

@sink(type='http', mapping='json', endpoint='http://localhost:9999/streamReceiver')
def stream OutputStream;

from StockStream
select symbol, price
insert into OutputStream;
```
