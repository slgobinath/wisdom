Selector selects attributes and events to be inserted into the following operator. If the selector is used with attribute names, it selects the attributes from events. If a selector is used with the index of events after windows, it selects the specified events from the list of events.

NOTE: Positive indices select events from the beginning of a list and negative indices select events from the end of a list. For example `0` selects the first event and `-1` selects the last event.

**Java API:**

Select `symbol` and `price` from all events and insert them into OutputStream.

```java
app.defineQuery("query1")
    .from("StockStream")
    .select("symbol", "price")
    .insertInto("OutputStream");
```

Select last two events from window and insert them into OutputStream.

```java
app.defineQuery("query1")
    .from("StockStream")
    .window.lengthBatch(5)
    .select(Index.of(-2, -1))
    .insertInto("OutputStream");
```

**Wisdom Query:**

Select `symbol` and `price` from all events and insert them into OutputStream.

```java
from StockStream
select symbol, price
insert into OutputStream;
```

Select last two events from window and insert them into OutputStream.

```java
from StockStream
select -2, -1
insert into OutputStream;
```
