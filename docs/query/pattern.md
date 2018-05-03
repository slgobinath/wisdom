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
