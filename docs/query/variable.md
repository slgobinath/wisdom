​
​​Similar to programming languages, Wisdom variables are used to store values in the memory. A variable can store any `java.lang.Comparable` objects.

## Variable Definition

​
Similar to streams, a variable must be defined before using it in a query. A variable definition must provide a unique name and a default value. Variable names should be lowercase, with words separated by underscores as necessary to improve readability.
 
**Java API:**
```java
WisdomApp app = new WisdomApp();
app.defineVariable("min_price", 10L);
```

**Wisdom Query:**
```java
def variable min_price;
```

## Read Variable

In Wisdom Java API, variables inherit `java.util.function.Supplier`. Therefore, anywhere you need a Supplier, you can use a Variable. In the following examples, a variable is used to define a dynamic filter.

**Java API:**
```java
WisdomApp app = new WisdomApp();
app.defineStream("StockStream");
app.defineStream("OutputStream");
Variable<Comparable> minPrice = app.defineVariable("min_price", 10L);

app.defineQuery("query1")
    .from("StockStream")
    .filter(Operator.GREATER_THAN("price", minPrice))
    .insertInto("OutputStream");
```

**Wisdom Query:**
```java
def stream StockStream;
def stream OutputStream;
def variable min_price = 10;

from StockStream
filter price > $min_price
insert into OutputStream;
```

Note the `$` prefix to variable name in the above query to indicate the difference between attribute name and variable.

## Update Variable

A variable can be updated externally using Java API or using an event in a query. To update a variable from an event, the event should have an attribute with the same name as the variable. Following queries, update the variable `min_price` with an assumption that events in `VariableStream` have an attribute `value`.

**Java API:**

A dynamic size length window defined using a variable.

```java
WisdomApp app = new WisdomApp();
app.defineStream("VariableStream");
app.defineVariable("min_price", 3);

app.defineQuery("query")
    .from("VariableStream")
    .map(Mapper.rename("value", "min_price"))
    .update("min_price");
```

**Wisdom Query:**

A dynamic size lengthBatch window defined using a variable.

```java
def stream VariableStream;
def variable min_price = 3;

from VariableStream
map value as min_price
update $min_price;
```