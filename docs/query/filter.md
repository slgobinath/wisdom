Filter is an operator to filter events coming from a stream. In Wisdom query, a `filter` can be used anywhere in between `from` and `insert into` statements.

In Java API, the `filter` method accepts any `java.util.function.Predicate<Event>` as the argument. For user's convenient, Wisdom offers some built-in predicates:

- Operator.EQUALS
- Operator.GREATER_THAN
- Operator.GREATER_THAN_OR_EQUAL
- Operator.LESS_THAN
- Operator.LESS_THAN_OR_EQUAL
- Operator.STR_IN_ATTR and its variants

**Java API:**

Filter events having `symbol` equal to `AMAZON`.

```java
app.defineQuery("query1")
    .from("StockStream")
    .filter(event -> "UWO".equals(event.get("symbol")))
    .insertInto("OutputStream");
```

Above code can be written using built-in `Operator.EQUALS` predicate as shown below:

```java
app.defineQuery("query1")
    .from("StockStream")
    .filter(Operator.EQUALS("symbol", "AMAZON"))
    .insertInto("OutputStream");
```

**Wisdom Query:**

Filter events having `symbol` equal to `AMAZON`.

```java
from StockStream
filter symbol == 'AMAZON'
insert into OutputStream;
```

Wisdom Query supports the following logical operators: `==`, `>`, `>=`, `<`, `<=` and `in`
