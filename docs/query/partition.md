Partitions split streams into partitions based on given attributes to sandbox aggregations and to parallelize execution.

**Java API:**

Partition `StockStream` by `symbol`

```java
app.defineQuery("query1")
    .from("StockStream")
    .partitionBy("symbol")
    .select("symbol", "price")
    .insertInto("OutputStream");
```

**Wisdom Query:**

Rename `symbol` to `name` and `price` to `cost`.

```java
from StockStream
partition by symbol
select symbol, price
insert into OutputStream;
```