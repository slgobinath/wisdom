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