A Wisdom application can be created either using Java API or Wisdom Query Language. Wisdom Query Language is a SQL like query inspired by [Siddhi Query](https://wso2.github.io/siddhi/). A Wisdom query should follow this template:

```text
<app annotation>?
( <stream definition> | <variable definition> | ... ) + 
( <query> ) +
;
```

**NOTE:** Since I am busy with my research, I am unable to list all features implemented in Wisdom here. Once you get access to Wisdom, please check the Unit test classes to see the available features and how to use them. You can contact me at any time to clarify your issues.

# Stream

Stream is the most basic component of stream processor. In Wisdom, you need to define a stream before using it. Wisdom streams are dynamically typed like Python so you cannot define attributes of a stream. At the runtime, whatever you pass will be accepted by the stream.

**Java API:**
```java
app.defineStream("StockStream");
```

**Wisdom Query:**
```java
def stream StockStream;
```
## Select

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

## Aggregate

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

# Filter

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

## Window

Windows are used to batch events based on some conditions. Wisdom 0.0.1 supports the following windows:

- Window.length
- Window.lengthBatch
- Window.externalTimeBatch
- UniqueWindow.lengthBatch
- UniqueWindow.externalTimeBatch

**Java API:**

```java
app.defineQuery("query1")
    .from("StockStream")
    .window(Window.lengthBatch(3))
    .insertInto("OutputStream");
```

**Wisdom Query:**

```java
from StockStream
window.lengthBatch(3)
insert into OutputStream;
```

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

## Partition

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

## Pattern

Patterns are another important aspect of stream processing. Wisdom supports regular patterns, count patterns and logical patterns.

**Java API:**

Wisdom pattern to detect `IBM` or `WSO2` followed by `ORACLE`.

```java
app.defineStream("StockStream");
app.defineStream("OutputStream");

// e1 or e2 -> e3
Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream")
        .filter(event -> event.get("symbol").equals("IBM"));
Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream")
        .filter(event -> event.get("symbol").equals("WSO2"));
Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream")
        .filter(event -> event.get("symbol").equals("ORACLE"));

Pattern finalPattern = Pattern.followedBy(Pattern.or(e1, e2), e3);

app.defineQuery("query1")
    .from(finalPattern)
    .select("e1.symbol", "e2.symbol", "e3.symbol")
    .insertInto("OutputStream");
```

Still I have not implemented Wisdom Query for pattern. Therefore, patterns can be used only in Java.

## Source

Source is an event source for a Wisdom stream. Currently Wisdom provides the following sources:

- HTTP
- Kafka
- CSV
- Pcap

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

## Sink

Sink receives events from a stream and send them to the relative receiver. Currently Wisdom supports the following sinks:

- HTTP
- Kafka
- CSV
- Text File
- Console

**Java API:**

Define HTTP sink in Java.

```java
wisdomApp.defineQuery("query1")
        .from("StockStream")
        .select("symbol", "price")
        .insertInto("OutputStream");

wisdomApp.addSink("OutputStream", new HTTPSink("http://localhost:9999/streamReceiver"));
```

**Wisdom Query:**

Define HTTP sink in Wisdom Query.

```java
def stream StockStream;

@sink(type='http', mapping='json', endpoint='http://localhost:9999/streamReceiver')
def stream OutputStream;

from StockStream
select symbol, price
insert into OutputStream;
```

## Variable

Wisdom variables are used to store a value in memory. A Wisdom query can be defined using predefined variables.

**Java API:**

A dynamic size length window defined using a variable.

```java
app.defineStream("StockStream");
app.defineStream("OutputStream");
app.defineStream("VariableStream");
Variable<Integer> variable = app.defineVariable("window_length", 3);

app.defineQuery("query1")
    .from("StockStream")
    .filter(Operator.GREATER_THAN("price", 55.0))
    .window(Window.lengthBatch(variable))
    .select("symbol", "price")
    .insertInto("OutputStream");

app.defineQuery("query2")
    .from("VariableStream")
    .filter(Operator.GREATER_THAN("value", 0))
    .map(Mapper.rename("value", "window_length"))
    .update("window_length");
```

**Wisdom Query:**

A dynamic size lengthBatch window defined using a variable.

```java
def stream StockStream;
def stream OutputStream;
def stream VariableStream;
def variable window_length = 3;

from StockStream
filter price > 55.0
window.lengthBatch($window_length)
select symbol, price
insert into OutputStream;

from VariableStream
filter value > 0
map value as window_length
update window_length;
```