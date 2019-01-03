There are plenty of stream processors out there but Wisdom does not just increase the count. It is adaptive, distributable and self-boosting without compromising the performance.

**Key Features:**

- Self-tuning queries
- Functionally auto-scaling deployment
- High throughput
- Lightweight
- Distributed
- Open Source

## Adaptive and Tunable

One of the key selling point of Wisdom is its ability to tune queries for best results. Wisdom queries can be defined using variables and Wisdom will tune them based on a loss function you define. For example, we defined three [Wisdom rules](use-cases/intrusion-detection.md) to detect intrusions but in none of them we defined the time window interval or the minimum count threshold. Instead, Wisdom mined and optimized those values from training data. Obtained optimal values gave us **99% accuracy** in intrusion detection. Isn't that cool?

For more details about the optimization algorithm, please read [Real-time Intrusion Detection in Network Traffic Using Adaptive and Auto-scaling Stream Processor](https://www.researchgate.net/publication/326969312_Real-time_Intrusion_Detection_in_Network_Traffic_Using_Adaptive_and_Auto-scaling_Stream_Processor?_sg=wxVvIi51niOx4OCdGXl27RbzK88K4ubWNhdVLSMsC544DS2PrGuBqWfjzEAhBXlr2rFBLYnX72GNsO6JdW3nRFKKbRoHZHqtyOFNEyzV.hC3j1u8IxL4s7LnGKZ3UiEjdbFF8XYAeQEPEddw5EdP3J5cfQyhqzC28O-82f9vdDfbYCa2O_SfIuG1tGKZDBA)

## Performance

A common limitation I have observed in dynamic stream processors 
is their performance bottleneck. I designed the underlying architecture of Wisdom in such a way that it is comparable with existing commercial stream processors. I developed a simple filter query to compare Wisdom with [Apache Flink](https://flink.apache.org/), [WSO2 Siddhi](https://wso2.github.io/siddhi/) and [Esper CEP](http://www.espertech.com/esper/). The throughput and latency of **Wisdom is closed to WSO2 Siddhi and better than Esper**.


Stream Processor |       Throughput     |      Latency
-----------------|----------------------|----------------
Apache Flink     | 6,711,544 events/sec | 100 nanoseconds
WSO2 Siddhi      | 3,811,876 events/sec | 216 nanoseconds
Wisdom           | 2,543,299 events/sec | 332 nanoseconds
Esper            | 2,247,807 events/sec | 334 nanoseconds


## Functionally Auto-scaling

In our research, we have shown that Wisdom consumes significantly fewer system resources than other distributed stream processors. The complete deployment setup and how it works are described in our research paper.

## Wisdom Query

Wisdom Query is an SQL like query inspired by [Siddhi Query](https://wso2.github.io/siddhi/). An expressive query language hides the complexity of stream processing and make it super easy to use stream processors. In addition, it lets you deploy CEP applications via REST API calls instead of transferring a deployable `jar` file or any other binary file.