Wisdom Filter filters events from a stream based on a given predicate. In Wisdom query, a `filter` can be used anywhere in between `from` and `insert into` or `update` statements.

In Java API, the `filter` method accepts any `java.util.function.Predicate<Event>` as the argument. For user's convenient, Wisdom offers the following built-in predicates:

| Java API        | Query Operator           | Description  |
| --------------------------------- |:-----------------:| :----------------------------------------------------------------|
| Operator.EQUALS 					| `==` 		| Checks if left operand is equal to right operand |
| Operator.GREATER_THAN 			| `>` 		| Checks if left operand is greater than right operand |
| Operator.GREATER_THAN_OR_EQUAL 	| `>=` 		| Checks if left operand is greater than or equal to right operand |
| Operator.LESS_THAN 				| `<` 		| Checks if left operand is less than right operand |
| Operator.LESS_THAN_OR_EQUAL 	 	| `<=` 		| Checks if left operand is less than or equal to right operand |
| Operator.IN 	 					| `in` 		| Checks if left operand is in right operand. Here the right operand can be a string or array |
| Operator.MATCHES 					| `matches` | Checks if left regex matches in the right string |

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
    .filter(Operator.EQUALS(Attribute.of("symbol"), "AMAZON"))
    .insertInto("OutputStream");
```

**Wisdom Query:**

Filter events having `symbol` equal to `AMAZON`.

```java
from StockStream
filter symbol == 'AMAZON'
insert into OutputStream;
```

Wisdom Query supports the following logical operators: `==`, `>`, `>=`, `<`, `<=`, `in` and `matches`
