# Wisdom Stream Processor

Wisdom is an adaptive, distributed and functionally auto-scaling stream processor 
written in Java 11 using modern architecture from the scratch.
Though I designed and developed Wisdom for my research project, my OCD in designing 
scalable and extensible architecture made Wisdom an industry ready product 
and comparable to other commercial stream processors. 
I admit that Wisdom may lack some features that you are looking for. 
For example, right now Wisdom does not support database integration. 
It is only because my research did not require those features and 
I do not have enough time to add those extra nice-to-have features.
Feel free to write your own extensions to implement the missing features using [Wisdom Extension API](https://slgobinath.github.io/wisdom/wisdom-extensions).

For more details, visit https://slgobinath.github.io/wisdom/

## Citation Required
Wisdom is published under Apache 2.0 license with an additional requirement:

**If you are using Wisdom for your research, you must cite Wisdom in your paper as given below:**

```text
Loganathan, G., Samarabandu, J., & Wang, X. (2018). Real-time Intrusion Detection in Network Traffic Using Adaptive and Auto-scaling Stream Processor. In 2018 IEEE Global Communications Conference (GLOBECOM) (GLOBECOM 2018). Abu Dhabi, UAE.
```

BibTex
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

## License
Apache 2.0