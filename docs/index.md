# Wisdom Stream Processor

Wisdom is an adaptive, distributed and functionally auto-scaling stream processor written in Java 11 using modern architecture from the scratch.
Though I designed and developed Wisdom for my research project, my OCD in designing scalable and extensible architecture made Wisdom 
an industry ready product with all basic requirements. I admit that Wisdom may lack some features that you are looking for. 
For example, right now Wisdom does not support database integration. It is only because my research did not require those 
features and I do not have enough time to add those extra nice-to-have features.


## Research Work

Query optimization and functionally auto-scaling deployment of Wisdom Stream Processor are published in the GLOBECOM 2018 conference.
You can find more details about the research in [Real-time Intrusion Detection in Network Traffic Using Adaptive and Auto-scaling Stream Processor](https://www.researchgate.net/publication/326969312_Real-time_Intrusion_Detection_in_Network_Traffic_Using_Adaptive_and_Auto-scaling_Stream_Processor?_sg=wxVvIi51niOx4OCdGXl27RbzK88K4ubWNhdVLSMsC544DS2PrGuBqWfjzEAhBXlr2rFBLYnX72GNsO6JdW3nRFKKbRoHZHqtyOFNEyzV.hC3j1u8IxL4s7LnGKZ3UiEjdbFF8XYAeQEPEddw5EdP3J5cfQyhqzC28O-82f9vdDfbYCa2O_SfIuG1tGKZDBA) for more details.
If you are using Wisdom for your research work, please cite Wisdom using the following paper:

**Citation:**

```text
Loganathan, G., Samarabandu, J., & Wang, X. (2018). Real-time Intrusion Detection in Network Traffic Using Adaptive and Auto-scaling Stream Processor. In 2018 IEEE Global Communications Conference (GLOBECOM) (GLOBECOM 2018). Abu Dhabi, UAE.
```

**BibTex**

```bibtex
@INPROCEEDINGS{Gobinath:Wisdom,
AUTHOR="Gobinath Loganathan and Jagath Samarabandu and Xianbin Wang",
TITLE="Real-time Intrusion Detection in Network Traffic Using Adaptive and Auto-scaling Stream Processor",
BOOKTITLE="2018 IEEE Canadian Conference on Electrical \& Computer Engineering (CCECE)
(CCECE 2018)",
ADDRESS="Abu Dhabi, UAE",
DAYS=13,
MONTH=dec,
YEAR=2018,
ABSTRACT="Advanced intrusion detection systems are beginning to utilize the power and flexibility offered by Complex Event Processing (CEP) engines. 
Adapting to new attacks and optimizing CEP rules are two challenges in this domain. 
Optimizing CEP rules requires a complete framework which can be ported to stream processors because a CEP rule cannot run without a stream processor. 
External dependencies of stream processors make CEP rule a black box which is hard to optimize. 
In this paper, we present a novel adaptive and functionally auto-scaling stream processor: "Wisdom" with a 
built-in hybrid optimizer developed using Particle Swarm Optimization, and Bisection algorithms to optimize CEP rule parameters. 
We show that an adaptive ``Wisdom'' rule tuned by the proposed optimization algorithm is able to detect selected attacks 
in CICIDS 2017 dataset with an average precision of 99.98\% and an average recall of 93.42\% while processing over 
2.5 million events per second. The proposed distributed functionally auto-scaling deployment mode consumes significantly 
fewer system resources than the monolithic deployment of CEP rules."
}
``` 


## About the Author

Gobinath is a research assistant at the University of Western Ontario, Canada. He completed his bachelor's degree at the University of Moratuwa, Sri Lanka with a first class and joined [WSO2](https://wso2.com/) a middleware company providing open source solutions for agile digital business. He spent 9 months in WSO2 Research and Development Analytics team where he had on hand experience on stream processing. Though he used stream processor for his undergraduate research project, at WSO2 he designed and developed Complex Event Processing (CEP) operators for [Siddhi](https://wso2.github.io/siddhi/) CEP engine.

Siddhi is an open source CEP engine with tons of features and being used by over 60 companies including [UBER](http://wso2.com/library/conference/2017/2/wso2con-usa-2017-scalable-real-time-complex-event-processing-at-uber?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17). Following features were designed and developed by Gobinath:

**Event Window**

A global window that can be reused to avoid duplicating windows in more than one places.

```
define stream SensorStream (name string, value float, roomNo int, deviceID string);
define stream KitchenSensorStream (name string, value float, deviceID string);
define window SensorWindow (name string, value float, roomNo int, deviceID string) timeBatch(1 second);

@info(name = 'query0')
from SensorStream
insert into SensorWindow;

@info(name = 'query1')
from  KitchenSensorStream
select name, value, 10 as  roomNo, deviceID
insert into SensorWindow;
```

**Absent Event Pattern**

Logical pattern to detect events those have not arrived.
```
define stream CustomerStream (customerId string);

@info(name = 'query1')
from not CustomerStream for 1 sec
select *
insert into OutputStream;
```

**Siddhi Debugger**

A complete debugger to track events in Siddhi and to debug them along with a terminal endpoint.

```java
SiddhiManager siddhiManager = new SiddhiManager();

String cseEventStream = "@config(async = 'true') define stream cseEventStream (symbol string, price float, " +
        "volume int);";
final String query = "@info(name = 'query 1')" +
        "from cseEventStream " +
        "select symbol, price, volume " +
        "insert into OutputStream; ";

ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
siddhiDebugger.acquireBreakPoint("query 1", SiddhiDebugger.QueryTerminal.IN);

siddhiDebugger.setDebuggerCallback(new SiddhiDebuggerCallback() {
    @Override
    public void debugEvent(ComplexEvent event, String queryName, SiddhiDebugger.QueryTerminal queryTerminal,
                            SiddhiDebugger debugger) {
        log.info("Query: " + queryName + ":" + queryTerminal);
        log.info(event);
        debugger.next();
    }
});

inputHandler.send(new Object[]{"WSO2", 50f, 60});
inputHandler.send(new Object[]{"WSO2", 70f, 40});
executionPlanRuntime.shutdown();
```

**Siddhi Event Playback**

```
@Plan:playback
define stream cseEventStream (symbol string, price float, volume int);

@info(name = 'query1')
from cseEventStream#window.timeBatch(1 sec)
select *
insert all events into outputStream ;
```

**Siddhi Reverse Geocode Extension**

```
define stream LocationStream (deviceId string, timestamp long, latitude double, longitude double);
@info(name = 'query1')
from LocationStream#geo:reversegeocode(latitude, longitude)
select streetNumber, neighborhood, route, administrativeAreaLevelTwo, administrativeAreaLevelOne, country, countryCode, postalCode, formattedAddress
insert into OutputStream
```

**Proximity Marketting Usecase**

The proximity marketting use cased developed by Gobinath using WSO2 Stream Processor is being used to demonstrate the product to clients.

```java
@Plan:name('RealtimeAnalytics-ExecutionPlan-ProductOfferGenerator')

@Plan:description('Based on the proximity of the user, send offers to the user')


@Import('org.wso2.realtime.analytics.stream.CustomerLocation:1.0.0')
define stream CustomerLocationStream (meta_timestamp long, customerId string, floorNumber int, shelfNumber int);

@Export('org.wso2.realtime.analytics.stream.SendOffer:1.0.0')
define stream SendOfferStream (meta_userId string, meta_timestamp long, productName string, offerName string, offerDescription string, expirationDate long);

@IndexBy('id')
@From(eventtable='rdbms', datasource.name='WSO2_REALTIME_ANALYTICS_BEACON', table.name='ORG_WSO2_REALTIME_ANALYTICS_EVENT_TABLE_ITEM')
define table ItemEventTable (id string, name string, category string, floorNumber int, shelfNumber int, offerId string);

@IndexBy('id')
@From(eventtable='rdbms', datasource.name='WSO2_REALTIME_ANALYTICS_BEACON', table.name='ORG_WSO2_REALTIME_ANALYTICS_EVENT_TABLE_OFFER')
define table OfferEventTable (id string, name string, description string, expirationDate long);

/* Find if the user is in the same location continuously more than 5 seconds */
from every(loc1 = CustomerLocationStream) -> loc2 = CustomerLocationStream[(meta_timestamp > loc1.meta_timestamp) AND (floorNumber == loc1.floorNumber) AND (shelfNumber == loc1.shelfNumber)]<0:> -> loc3 = CustomerLocationStream[(meta_timestamp >= loc1.meta_timestamp + 30000) AND (floorNumber == loc1.floorNumber) AND (shelfNumber == loc1.shelfNumber)]
within 5 sec
select loc1.customerId as userId, loc3.meta_timestamp as timestamp, loc1.floorNumber as floorNumber, loc1.shelfNumber as shelfNumber
insert into #ProximityStream;

/* Find if the event is triggered for the same series of events. The same pattern identified within a day is ignored to avoid spaming. For testing purpose 5 minutes is used */
from #ProximityStream as leftStream left outer join #ProximityStream#window.time(5 minutes) as rightStream on leftStream.userId == rightStream.userId AND leftStream.floorNumber == rightStream.floorNumber AND leftStream.shelfNumber == rightStream.shelfNumber
select leftStream.userId as userId, leftStream.timestamp as timestamp, leftStream.floorNumber as floorNumber, leftStream.shelfNumber as shelfNumber, rightStream.userId IS NULL as isNewEvent
insert into #ProximityPerDayStream;

/* Allow only new event patterns to trigger offer */
from #ProximityPerDayStream[isNewEvent]
select userId, timestamp, floorNumber, shelfNumber
insert into #FilteredProximityStream;

/* Find whether the product has an offer */
from #FilteredProximityStream as proximity join ItemEventTable as item on proximity.floorNumber == item.floorNumber AND proximity.shelfNumber == item.shelfNumber AND item.offerId != "N/A"
select proximity.userId as userId, proximity.timestamp as timestamp, item.id as itemId, item.name as productName, item.offerId as offerId
insert into #ItemStream;

/* Validate offer expiary peiod and send the offer */
from #ItemStream as item join OfferEventTable as offer on item.offerId == offer.id AND item.timestamp <= offer.expirationDate
select item.userId as meta_userId, item.timestamp as meta_timestamp, item.productName as productName, offer.name as offerName, offer.description as offerDescription, offer.expirationDate as expirationDate
insert into SendOfferStream;
```

In addition, he also fixed several concurrency issues and HA deployment issues. Later, he moved to Canada for his higher studies and developed his own stream processor: `Wisdom` for his research requirement. Wisdom can optimize itself for better results and self-boost for resource utilization.

If you like to invest in Wisdom, please contact me via `slgobinath@gmail.com`. If you are looking for an easy to use stream processor with fresh design and less complexity for your research, you are at the right place. Just drop me an email: `slgobinath@gmail.com`.

