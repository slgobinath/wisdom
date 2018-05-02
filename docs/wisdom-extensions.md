Writing extensions for Wisdom is super easy if you know Java. An extension can be a `Window`, `Source`, `Sink`, or `Mapper`. In this section, I explain how to create a new sink to write events to a text file. All existing windows, sources, sinks, and mappers are written as extensions following the same technique.

## Create New Sink Extension

**Step 1:** Create a new Maven project in your favorite IDE.

**Step 2:** Open `pom.xml` file and add `wisdom-core` dependency as show below:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.javahelps</groupId>
    <artifactId>wisdom-java-api</artifactId>
    <version>1.0-SNAPSHOT</version>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>1.9</source>
                    <target>1.9</target>
                </configuration>
            </plugin>
        </plugins>
    </build>

    <properties>
        <wisdom.version>0.0.1</wisdom.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.javahelps.wisdom</groupId>
            <artifactId>wisdom-core</artifactId>
            <version>${wisdom.version}</version>
        </dependency>
    </dependencies>

</project>
```

**Step 3:** Create a new class `com.javahelps.wisdom.extensions.file.sink.TextFileSink` and extend `com.javahelps.wisdom.core.stream.output.Sink`. Depending on your extension type, you may need to override different classes:

- Window - `com.javahelps.wisdom.core.window.Window`
- Source - `com.javahelps.wisdom.core.stream.input.Source`
- Sink - `com.javahelps.wisdom.core.stream.output.Sink`
- Mapper - `com.javahelps.wisdom.core.map.Mapper`

```java
package com.javahelps.wisdom.extensions.file.sink;

import com.javahelps.wisdom.core.stream.output.Sink;

public class TextFileSink extends Sink {

}
```

**Step 4:** Annotate the class using `WisdomExtension` annotation and define namespace as `file.text` which will be used to identify this sink later in Wisdom query. This step is common for all Wisdom extensions.

```java
package com.javahelps.wisdom.extensions.file.sink;

import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.stream.output.Sink;

@WisdomExtension("file.text")
public class TextFileSink extends Sink {

}
```

**Step 5:** Override all required methods. For text file sink, overriding `publish` method is enough. You may need `start`, `init` and `stop` if your sink is complex as Kafka sink.

```java
package com.javahelps.wisdom.extensions.file.sink;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.stream.output.Sink;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

@WisdomExtension("file.text")
public class TextFileSink extends Sink {

    private final String path;

    public TextFileSink(Map<String, ?> properties) {
        super(properties);
        this.path = (String) properties.get("path");
        if (this.path == null) {
            throw new WisdomAppValidationException("Required property 'path' for TextFile sink not found");
        }
    }

    @Override
    public void start() {
        // Do nothing
    }

    @Override
    public void init(WisdomApp wisdomApp, String streamId) {

    }

    @Override
    public void publish(List<Event> events) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(this.path, true))) {
            for (Event event : events) {
                writer.println(event);
            }
        }
    }

    @Override
    public void stop() {
        // Do nothing
    }
}
```

Now you are ready to use this sink in your Wisdom app.

## Use Extension in Java API

**Step 1:** Create a new test class in the same project `com.javahelps.wisdom.extensions.file.sink.TestTextFileSink`.

**Step 2:** Create a static initialization block and import the custom extension.

```java
package com.javahelps.wisdom.extensions.file.sink;

import com.javahelps.wisdom.core.extension.ImportsManager;

public class TestTextFileSink {

    static {
        ImportsManager.INSTANCE.use(TextFileSink.class);
    }
}
```

We use `ImportsManager` to import selected extension, instead of searching the complete classpath to avoid unnecessary delays. It also reduces unnecessary complexities in Android applications.

**Step 3:** Create a new Wisdom app using the `file.text` sink. Note that we are using the namespace `file.text` to create this sink.

```java
WisdomApp wisdomApp = new WisdomApp();
wisdomApp.defineStream("StockStream");
wisdomApp.defineStream("OutputStream");

wisdomApp.defineQuery("query1")
        .from("StockStream")
        .select("symbol", "price")
        .insertInto("OutputStream");
wisdomApp.addSink("OutputStream", Sink.create("file.text", Map.of("path", "output.log")));


wisdomApp.start();

InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
```

## Use Extension in Wisdom Query

Above sink can be used in a Wisdom query as given below:

```java
def stream StockStream;
@sink(type='file.text', path='output.log')
def stream OutputStream;

from StockStream
select symbol, price
insert into OutputStream;JAR
```

## Deploy in Wisdom Server

**Step 1:** Build the jar file containing `com.javahelps.wisdom.extensions.file.sink.TextFileSink`.

```bash
mvn clean package
```

**Step 2:** Copy and paste the `target/xxx.jar` file into `WISDOM_HOME/lib` directory.

**Step 3:** Restart running Wisdom services.