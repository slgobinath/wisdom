Wisdom offers a complete Java API and Wisdom query to develop Complex Event Processing (CEP) applications. Wisdom can be used as a Java library or standalone service. Java library is recommended for testing purposes, small scale applications and Android applications. If you are developing a resource consuming CEP application, it is recommended to use Wisdom Service. Wisdom Service is the only way to use HTTP Source and Sinks. This section explains how to create a simple CEP application using Wisdom Java API and Wisdom Query.

## Requirements

Make sure that you have set up the following softwares in your system before building Wisdom.

- Java 11 (or latest)
- Apache Maven
- Apache Kafka (for self-boosting deployment)

## Installation

Please contact me (`slgobinath@gmail.com`) to get access to Wisdom source code. Once you have downloaded the Wisdom source code, follow these steps to build and install Wisdom library.

Open your terminal and change directory

```bash
cd wisdom
```

Compile and install Wisdom using Apache Maven

```bash
mvn clean install
```

## Wisdom Java API

Create a new Maven Project in your favorite IDE. We use [IntelliJ IDEA](https://www.jetbrains.com/idea/) throughout this document.

Open the `pom.xml` file add `wisdom-core` and optionally `logback` dependencies as shown below:

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
                    <source>11</source>
                    <target>11</target>
                </configuration>
            </plugin>
        </plugins>
    </build>

    <properties>
        <wisdom.version>0.0.1</wisdom.version>
        <slf4j.version>1.7.25</slf4j.version>
        <logback.version>1.2.3</logback.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.javahelps.wisdom</groupId>
            <artifactId>wisdom-core</artifactId>
            <version>${wisdom.version}</version>
        </dependency>

        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-core</artifactId>
            <version>${logback.version}</version>
        </dependency>

        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>${logback.version}</version>
        </dependency>
    </dependencies>

</project>
```

Create a new Java class `com.javahelps.helloworld.javaapi.HelloWorld` with the following code.

```java
package com.javahelps.helloworld.javaapi;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.operator.Operator;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.core.util.EventPrinter;

public class HelloWorld {

    public static void main(String[] args) {

        // Create a Wisdom application
        WisdomApp app = new WisdomApp("WisdomApp", "1.0.0");

        // Define streams
        app.defineStream("StockStream");
        app.defineStream("OutputStream");

        // Create a query
        app.defineQuery("FilterQuery")
                .from("StockStream")
                .filter(Operator.EQUALS("symbol", "AMAZON"))
                .select("symbol", "price")
                .insertInto("OutputStream");

        // Add output stream callback
        app.addCallback("OutputStream", EventPrinter::print);

        // Get an input handler
        InputHandler inputHandler = app.getInputHandler("StockStream");

        // Start the application
        app.start();

        // Send three inputs
        inputHandler.send(EventGenerator.generate("symbol", "GOOGLE", "price", 10.5, "volume", 10L));
        inputHandler.send(EventGenerator.generate("symbol", "AMAZON", "price", 20.5, "volume", 20L));
        inputHandler.send(EventGenerator.generate("symbol", "FACEBOOK", "price", 30.5, "volume", 30L));

        // Shutdown the application
        app.shutdown();
    }
}
```

Above code creates Wisdom application with two streams: `StockStream` and `OutputStream`, and a query named `FilterQuery`. The `FilterQuery` filters stock events of `AMAZON`, select `symbol` and `price`, and insert them into the `OutputStream`. `InputHandler` is used to feed events to a stream and callback is used to receive events from a stream.

Running this code should print an output similar to this:

```txt
[Event{timestamp=1524709449322, stream=OutputStream, data={symbol=AMAZON, price=20.5}, expired=false}]
```

As you can see, above Wisdom app filters events having symbol equal to AMAZON and prints them to the console.

## Wisdom Query

Above Wisdom application can be defined using the folloing Wisdom query:

```java
@app(name='WisdomApp', version='1.0.0')
def stream StockStream;
def stream OutputStream;

@query(name='FilterQuery')
from StockStream
filter symbol == 'AMAZON'
select symbol, price
insert into OutputStream;
```

To use this query in a Java application, create a new Maven project in your favorite IDE.

Open the `pom.xml` file and add `wisdom-core`, `wisdom-query` and optionally `logback` dependencies as shown below:

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
                    <source>11</source>
                    <target>11</target>
                </configuration>
            </plugin>
        </plugins>
    </build>

    <properties>
        <wisdom.version>0.0.1</wisdom.version>
        <slf4j.version>1.7.25</slf4j.version>
        <logback.version>1.2.3</logback.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.javahelps.wisdom</groupId>
            <artifactId>wisdom-core</artifactId>
            <version>${wisdom.version}</version>
        </dependency>

        <dependency>
            <groupId>com.javahelps.wisdom</groupId>
            <artifactId>wisdom-query</artifactId>
            <version>${wisdom.version}</version>
        </dependency>

        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-core</artifactId>
            <version>${logback.version}</version>
        </dependency>

        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>${logback.version}</version>
        </dependency>
    </dependencies>

</project>
```

Create a new Java class `com.javahelps.helloworld.wisdomql.HelloWorld` with the following code.

```java
package com.javahelps.helloworld.wisdomql;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.core.util.EventPrinter;
import com.javahelps.wisdom.query.WisdomCompiler;

public class HelloWorld {

    public static void main(String[] args) {

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                " " +
                "@query(name='FilterQuery') " +
                "from StockStream " +
                "filter symbol == 'AMAZON' " +
                "select symbol, price " +
                "insert into OutputStream;";

        // Create a Wisdom application
        WisdomApp app = WisdomCompiler.parse(query);

        // Add output stream callback
        app.addCallback("OutputStream", EventPrinter::print);

        // Get an input handler
        InputHandler inputHandler = app.getInputHandler("StockStream");

        // Start the application
        app.start();

        // Send three inputs
        inputHandler.send(EventGenerator.generate("symbol", "GOOGLE", "price", 10.5, "volume", 10L));
        inputHandler.send(EventGenerator.generate("symbol", "AMAZON", "price", 20.5, "volume", 20L));
        inputHandler.send(EventGenerator.generate("symbol", "FACEBOOK", "price", 30.5, "volume", 30L));

        // Shutdown the application
        app.shutdown();
    }
}
```

Above code replaces the Java API used in previous example by the Wisdom query to construct a Wisdom app. Once the Wisdom app is created, creating InputHandler and sending events are same as the previous example.