A stream is the very basic component of stream processors. You can interpret a stream as the entry and exit points of a pipeline in a stream. Not like statically typed streams in other stream processors, Wisdom offers dynamically typed stream which can accept any attributes. However, an attribute of a stream can take only a `java.lang.Comparable` object. More details about supported data structures and how to pass lists will be covered later in another section.

## Stream Definition

​
Similar to variables, a stream must be defined before using it in a query. A stream definition must provide a unique name. I recommend using `PascalCase` naming convention for stream names.
 
**Java API:**
```java
WisdomApp app = new WisdomApp();
app.defineStream("StockStream");
```

**Wisdom Query:**
```java
def stream StockStream;
```

## Stream in Query

A Wisdom query must start with either a [Stream](stream.md) or [Pattern](pattern.md) and ends with a [Stream](stream.md) or [Variable](variable.md). In the following example, we fetch events from `StockStream` and feed them to `OutputStream`.

**Java API:**
```java
WisdomApp app = new WisdomApp();
app.defineStream("StockStream");
app.defineStream("OutputStream");
Variable<Comparable> min_price = app.defineVariable("min_price", 10L);

app.defineQuery("query1")
    .from("StockStream")
    .insertInto("OutputStream");
```

**Wisdom Query:**
```java
def stream StockStream;
def stream OutputStream;

from StockStream
insert into OutputStream;
```