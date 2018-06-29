Partitions split streams into partitions based on given attributes to sandbox aggregations and to parallelize execution. 

Wisdom supports two types of partitions.

1. Partition by attribute
2. Partition by value

Partition by attribute is similar to other stream processors, generates partition keys by concatenating their attribute values. For example, the following query partitions packets based on their `srcIp` and `dstIp`. Packets sent from `127.0.0.1` to `127.0.0.2` are assigned to a different partition from the packets sent from `127.0.0.2` to `127.0.0.1`.

Partition packets transferred from the same source to the same destination.

```java
from PacketStream
partition by srcIp, dstIp
select srcIp, dstIp, timestamp
insert into OutputStream;
```

Partition by value partitions events based on their actual values regardless of from which attributes they are from. For example, the following query partitions packets based on their `srcIp` and `dstIp`. Packets sent from `127.0.0.1` to `127.0.0.2` and packets sent from `127.0.0.2` to `127.0.0.1` are assigned to the same partition.

Partition packets transferred between the same source and destination.

```java
from PacketStream
partition by srcIp + dstIp
select srcIp, dstIp, timestamp
insert into OutputStream;
```

Both partition by attribute and partition by value behaves in the way if the number of attributes used to partition is one.

**Java API:**

Partition `StockStream` by `symbol`

```java
app.defineQuery("query1")
    .from("StockStream")
    .partitionByAttr("symbol")
    .select("symbol", "price")
    .insertInto("OutputStream");
```

```java
app.defineQuery("query1")
    .from("StockStream")
    .partitionByVal("symbol")
    .select("symbol", "price")
    .insertInto("OutputStream");
```

**Wisdom Query:**

Partition `StockStream` by `symbol`

```java
from StockStream
partition by symbol
select symbol, price
insert into OutputStream;
```