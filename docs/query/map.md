Wisdom map is used to map events from one format to another format. In Java API, mapper accepts any `java.util.function.Function<Event, Event>`. Wisdom also provides the following built-in mappers:

Wisdom Core library provides the built-in mappers. 

- Mapper.RENAME
- Mapper.FORMAT_TIME
- Mapper.TO_INT
- Mapper.TO_LONG
- Mapper.TO_FLOAT
- Mapper.TO_DOUBLE
- Mapper.CONSTANT

You can find the following mappers in the Wisdom Extension library:

- tensorFlow(`<model-path>`, `operation`, `type`) - Used to call a Tensorflow model and insert the result into the stream.
- grpc(`endpoint`, `select`) - Used to call a gRPC service and to insert the result into the stream
- http(`endpoint`, `method` select`) - Used to call an HTTP service and to insert the result into the stream


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

### Conditional Mapping
A map operator can be called only if a given condition is satisfied. A sample code to replace `null` symbol in a StockStream by `UNKNOWN` is given below.

```java
app.defineQuery("query1")
    .from("StockStream")
    .map(Mapper.CONSTANT("UNKNOWN", "symbol").onlyIf(Operator.EQUALS(Attribute.of("symbol"), null)))
    .select("symbol", "price")
    .insertInto("OutputStream");
```

**Wisdom Query:**

Rename `symbol` to `name` and `price` to `cost`.

```java
from StockStream
map 'UNKNOWN' as symbol if symbol == null
select symbol, price
insert into OutputStream;
```