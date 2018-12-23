There are plenty of stream processors out there but Wisdom does not just increase the count. It is adaptive, distributable and self-boosting without compromising the performance.

## Adaptive and Tunable

One of the key selling point of Wisdom is its adaptiveness. Wisdom queries can be defined using variables and Wisdom will tune them based on a loss function you define. For example, we defined three [Wisdom rules](intrusion-detection.md) to detect intrusions but in none of them we defined the time window interval or the minimum count threshold. Instead, Wisdom mined and optimized those values from training data. Obtained optimal values gave us **99% accuracy** in intrusion detection. Isn't that cool?

Optimization algorithms and other techniques are submitted to a conference. I will share the link here once it is published.

## Performace

A common limitation I have observed in dynamic stream processors 
is their performance bottleneck. I designed the underlying architecure of Wisdom in such a way that it is comparable with existing commercial stream processors. I developed a simple filter query to compare Wisdom with [Apache Flink](https://flink.apache.org/), [WSO2 Siddhi](https://wso2.github.io/siddhi/) and [Esper CEP](http://www.espertech.com/esper/). The throughput and latency of **Wisdom is closed to WSO2 Siddhi and better than Esper**. Exact perfomance results are included in the research paper.

## Functionally Auto-scaling

In our research, we have shown that Wisdom consumes significantly fewer system resources than other distributed stream processors. The complete deployment setup and how it works are described in our research paper. However, I can leak that the proposed distributed deployment consumed **1.5 times less memory** than monolithic deployment.

## Wisdom Query

Wisdom Query is an SQL like query inspired by [Siddhi Query](https://wso2.github.io/siddhi/). An expressive query language hides the complexity of stream processing and make it super easy to use stream processors. In addition, it lets you deploy CEP applications via REST API calls instead of transfering a deployable `jar` file or any other binary file.