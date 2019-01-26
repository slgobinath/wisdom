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

If you like to invest in Wisdom, please contact me via `slgobinath@gmail.com`. If you are looking for an easy to use stream processor with fresh design and less complexity for your research, you are at the right place. Just drop me an email: `slgobinath@gmail.com`.

