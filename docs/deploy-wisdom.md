Wisdom offers three different deployment options: (1) in-app usage as a Java library, (2) stand-alone deployment as a microservice, (3) Wisdom Orchestra deployment.

## Wisdom Library

This is the recommended method to use Wisdom if you are developing a new Wisdom rule or if you want to playwith Wisdom. It is also recommended for applications require in-app complex event processing. Please check the [Getting Started](getting-started.md) guidelines to use Wisdom as a library.

## Wisdom Service

Wisdom Service is recommended if you are deploying a stand-alone CEP rule which requires HTTP endpoints and/or more system resources to be allocated. You can either use the Wisdom server to run your query or develop your own microservice to run your Wisdom app.

**Deploy Wisdom Query Using Wisdom Server**

**Step 1:** Email the author(`slgobinath@gmail.com`) and get the Wisdom Server pack.

**Step 2:** Extract the zip file and navigate into the extracted directory.

```bash
unzip product-wisdom-0.0.1.zip
cd product-wisdom-0.0.1
```

**Step 3:** Save the following Wisdom query into the `artifacts` directory with a name: `stock_filter.wisdomql`.

```java
@app(name='stock_filter', version='1.0.0')

@source(type='http', mapping='json')
def stream StockStream;

@sink(type='console')
def stream OutputStream;

@query(name='FilterQuery')
from StockStream
filter symbol == 'AMAZON'
select symbol, price
insert into OutputStream;
```

**Step 4:** Start the Wisdom Service on port `8080` using the following command:

```bash
sh wisdom-service.sh --port 8080 artifacts/stock_filter.wisdomql
```

**Step 5:** Using [Postman](https://www.getpostman.com/) or similar tools, send an event using HTTP POST request. For simplicity, we use `curl` to send the request.

```bash
curl -d '{"symbol": "AMAZON", "price": 120.0, "volume": 10}' -H "Content-Type: application/json" -X POST http://localhost:8080/WisdomApp/StockStream
```

After sending this request, you should see the the following output in the terminal running Wisdom service:

```txt
Event{timestamp=1524757628355, stream=OutputStream, data={symbol=AMAZON, price=120.0}, expired=false}
```

## Wisdom Orchestra

Wisdom Orchestra deployment is a fancy name I use to refer managing Wisdom instances using Wisdom Manager. Wisdom Manager is a specially designed tool to deploy and manage Wisdom services. It can be used to deploy stand-alone Wisdom services or to deploy self-boosting Wisdom environment.

Wisdom Manager often requires [Apache Kafka](https://kafka.apache.org/) to coordinate and communicate with Wisdom instances. Therefore, please setup and start Apache Kakfa before running Wisdom Manager.

**Step 1:** Download and extract the latest [Apache Kafka](https://kafka.apache.org/) anywhere in your system.

**Step 2:** Start Apache Kafka using the following two commands from `KAFKA_HOME`.
```bash
# Start Zookeeper server
sh bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server
sh bin/kafka-server-start.sh config/server.properties
```

**Step 3:** Open another terminal in `WISDOM_HOME` and start the Wisdom Manager.
```bash
sh wisdom-manager.sh
```

**Step 4:** Send an HTTP POST request with a Wisdom query along with a port to start that query.

```bash
curl -d "{\"query\": \"@app(name='stock_filter', version='1.0.0') \
@source(type='http', mapping='json') \
def stream StockStream; \
@sink(type='file.text', path='/tmp/OutputStream.txt') \
def stream OutputStream; \
@query(name='FilterQuery') \
from StockStream \
filter symbol == 'AMAZON' \
select symbol, price \
insert into OutputStream;\", \"port\": 8085}" -H "Content-Type: application/json" -X POST http://localhost:8080/WisdomManager/app
```

Note that the OutputStream sink is a text file: `/tmp/OutputStream.txt`.

**Step 5:** Start `stock_filter` by sending another POST request.

```bash
curl -X POST http://localhost:8080/WisdomManager/start/stock_filter
```

**Step 06:** Test `stock_filter` by sending a stock event.

```bash
curl -d '{"symbol": "AMAZON", "price": 120.0, "volume": 10}' -H "Content-Type: application/json" -X POST http://localhost:8085/WisdomApp/StockStream
```

After sending above event, you should have a file `/tmp/OutputStream.txt` with the following content:

```text
Event{timestamp=1524761284277, stream=OutputStream, data={symbol=AMAZON, price=120.0}, expired=false}
```

**Step 07:** Stop the `stock_filter` app

```bash
curl -X POST http://localhost:8080/WisdomManager/stop/stock_filter
```

**Step 08:** Delete the `stock_filter` app

```bash
curl -X DELETE http://localhost:8080/WisdomManager/app/stock_filter
```

**Step 09:** Stop the Wisdom Manager

```bash
curl -X POST http://localhost:8080/WisdomManager/stop
```