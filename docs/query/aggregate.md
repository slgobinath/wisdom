Aggregators aggregate events and inject results into the stream. Wisdom supports the following aggregators:

- SUM
- MIN
- MAX
- AVERAGE
- COUNT

**Java API:**

Find the total price of three stock events.

```java
app.defineQuery("query1")
    .from("StockStream")
    .window(Window.lengthBatch(3))
    .aggregate(Operator.SUM("price", "total"))
    .insertInto("OutputStream");
```

**Wisdom Query:**

Find the total price of three stock events.

```java
from StockStream
window.lengthBatch(3)
aggregate sum(price) as total
insert into OutputStream;
```
