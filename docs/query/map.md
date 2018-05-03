## Map

Wisdom map is used to map events from one format to another format. In Java API, mapper accepts any `java.util.function.Function<Event, Event>`. Wisdom also provides the following built-in mappers:

- Mapper.rename
- Mapper.formatTime
- Mapper.toInt
- Mapper.toLong
- Mapper.toFloat
- Mapper.toDouble

**Java API:**

Rename `symbol` to `name` and `price` to `cost`.

```java
app.defineQuery("query1")
    .from("StockStream")
    .map(Mapper.rename("symbol", "name"), Mapper.rename("price", "cost"))
    .select("name", "cost")
    .insertInto("OutputStream");
```

**Wisdom Query:**

Rename `symbol` to `name` and `price` to `cost`.

```java
from StockStream
map symbol as name, price as cost
select name, cost
insert into OutputStream;
```