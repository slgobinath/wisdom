## Map

Wisdom map is used to map events from one format to another format. In Java API, mapper accepts any `java.util.function.Function<Event, Event>`. Wisdom also provides the following built-in mappers:

### Wisdom Core

- Mapper.RENAME
- Mapper.FORMAT_TIME
- Mapper.TO_INT
- Mapper.TO_LONG
- Mapper.TO_FLOAT
- Mapper.TO_DOUBLE

**Java API:**

Rename `symbol` to `name` and `price` to `cost`.

```java
app.defineQuery("query1")
    .from("StockStream")
    .map(Mapper.RENAME("symbol", "name"), Mapper.RENAME("price", "cost"))
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