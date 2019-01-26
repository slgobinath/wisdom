Patterns are another important aspect of stream processing. Wisdom supports regular patterns, count patterns and logical patterns.

**Java API:**

Wisdom pattern to detect `IBM` followed by `GOOGLE`.

```java
app.defineStream("StockStream");
app.defineStream("OutputStream");

// e1 -> e2
Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream")
        .filter(event -> event.get("symbol").equals("IBM"));
Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream")
        .filter(event -> event.get("symbol").equals("GOOGLE"));

Pattern finalPattern = Pattern.followedBy(e1, e2);

app.defineQuery("query1")
    .from(finalPattern)
    .select("e1.symbol", "e2.symbol")
    .insertInto("OutputStream");
```

Wisdom pattern to detect `IBM` followed by `GOOGLE` within five minutes.

```java
app.defineStream("StockStream");
app.defineStream("OutputStream");

// e1 -> e2 within 300,000 milliseconds
Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream")
        .filter(event -> event.get("symbol").equals("IBM"));
Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream")
        .filter(event -> event.get("symbol").equals("GOOGLE"));

Pattern finalPattern = Pattern.followedBy(e1, e2, 5*60*1000);

app.defineQuery("query1")
    .from(finalPattern)
    .select("e1.symbol", "e2.symbol")
    .insertInto("OutputStream");
```

Wisdom pattern to detect `IBM` or `GOOGLE` followed by `ORACLE`.

```java
app.defineStream("StockStream");
app.defineStream("OutputStream");

// e1 or e2 -> e3
Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream")
        .filter(event -> event.get("symbol").equals("IBM"));
Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream")
        .filter(event -> event.get("symbol").equals("GOOGLE"));
Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream")
        .filter(event -> event.get("symbol").equals("ORACLE"));

Pattern finalPattern = Pattern.followedBy(Pattern.or(e1, e2), e3);

app.defineQuery("query1")
    .from(finalPattern)
    .select("e1.symbol", "e2.symbol", "e3.symbol")
    .insertInto("OutputStream");
```

Wisdom pattern to detect `IBM` and `GOOGLE` followed by `ORACLE`.

```java
app.defineStream("StockStream");
app.defineStream("OutputStream");

// e1 and e2 -> e3
Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream")
        .filter(event -> event.get("symbol").equals("IBM"));
Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream")
        .filter(event -> event.get("symbol").equals("GOOGLE"));
Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream")
        .filter(event -> event.get("symbol").equals("ORACLE"));

Pattern finalPattern = Pattern.followedBy(Pattern.and(e1, e2), e3);

app.defineQuery("query1")
    .from(finalPattern)
    .select("e1.symbol", "e2.symbol", "e3.symbol")
    .insertInto("OutputStream");
```

Wisdom pattern to detect `IBM` but no `GOOGLE` before `IBM`.

```java
app.defineStream("StockStream");
app.defineStream("OutputStream");

// not e1 -> e2
Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream")
        .filter(event -> event.get("symbol").equals("IBM"));
Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream")
        .filter(event -> event.get("symbol").equals("GOOGLE"));

Pattern finalPattern = Pattern.followedBy(Pattern.not(e1), e2);

app.defineQuery("query1")
    .from(finalPattern)
    .select("e2.symbol")
    .insertInto("OutputStream");
```

Wisdom pattern to detect two to five number of `IBM` followed by `GOOGLE`.

```java
app.defineStream("StockStream");
app.defineStream("OutputStream");

// e1<2:5> -> e2
Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream")
        .filter(event -> event.get("symbol").equals("IBM"))
        .times(2, 5);
Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream")
        .filter(event -> event.get("symbol").equals("GOOGLE"));

Pattern finalPattern = Pattern.followedBy(e1, e2);

app.defineQuery("query1")
    .from(finalPattern)
    .select("e1[0].symbol", "e2.symbol")
    .insertInto("OutputStream");
```

**Wisdom Query:**

Wisdom pattern to detect `IBM` followed by `GOOGLE`.

```java
def stream StockStream;
def stream OutputStream;

from StockStream[symbol == 'IBM'] as e1 -> StockStream[symbol == GOOGLE] as e2
select e1.symbol, e2.symbol
insert into OutputStream;
```

Wisdom pattern to detect `IBM` followed by `GOOGLE` within five minutes.

```java
def stream StockStream;
def stream OutputStream;

from StockStream[symbol == 'IBM'] as e1 -> StockStream[symbol == GOOGLE] as e2 within time.minutes(5)
select e1.symbol, e2.symbol
insert into OutputStream;
```

Wisdom pattern to detect `IBM` or `GOOGLE` followed by `ORACLE`.

```java
def stream StockStream;
def stream OutputStream;

from (StockStream[symbol == 'IBM'] as e1 or StockStream[symbol == GOOGLE] as e2) -> StockStream[symbol == ORACLE] as e3
select e1.symbol, e2.symbol, e3.symbol
insert into OutputStream;
```

Wisdom pattern to detect `IBM` and `GOOGLE` followed by `ORACLE`.

```java
def stream StockStream;
def stream OutputStream;

from (StockStream[symbol == 'IBM'] as e1 and StockStream[symbol == GOOGLE] as e2) -> StockStream[symbol == ORACLE] as e3
select e1.symbol, e2.symbol, e3.symbol
insert into OutputStream;
```

Wisdom pattern to detect `IBM` but no `GOOGLE` before `IBM`.

```java
def stream StockStream;
def stream OutputStream;

from not StockStream[symbol == 'IBM'] -> StockStream[symbol == GOOGLE] as e2
select e2.symbol
insert into OutputStream;
```

Wisdom pattern to detect two to five number of `IBM` followed by `GOOGLE`.

```java
def stream StockStream;
def stream OutputStream;

from not StockStream[symbol == 'IBM']<2:5> as e1 -> StockStream[symbol == GOOGLE] as e2
select e1[0].symbol, e2.symbol
insert into OutputStream;
```