Source is an event source for a Wisdom stream. Currently Wisdom provides the following sources:

- HTTP
- Kafka
- CSV
- Pcap
- GRPC

**Java API:**

Define Kafka source in Java.

```java
wisdomApp.defineQuery("query1")
        .from("StockStream")
        .select("symbol", "price")
        .insertInto("OutputStream");

wisdomApp.addSource("StockStream", new KafkaSource("localhost:9092"));
```

**Wisdom Query:**

Define Kafka source in Wisdom Query.

```java
@source(type='kafka', bootstrap='localhost:9092')
def stream StockStream;

def stream OutputStream;

from StockStream
select symbol, price
insert into OutputStream;
```