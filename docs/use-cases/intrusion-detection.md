# Intrusion Detection

Following queries were developed to detect network attacks in [CICIDS 2017](http://www.unb.ca/cic/datasets/ids-2017.html) dataset. We developed the following rules based on facts behind each attacks. Even though we obtained an average precision of 99.98%, we are not responsible for failure of detecting real time attacks.

## FTP Brute Force Attack

In FTP Brute Force attack, an attacker tries different combinations of username and password to login to the FTP server. Therefore, there should be significantly large amount of failed attempts within a short period of time.

```java
@app(name='FTPBruteForceDetector', version='1.0.0')
def stream PacketStream;
def stream AttackStream;

def variable time_threshold = time.sec(1);
def variable count_threshold = 7;

from PacketStream
    filter 'FTP' == app_protocol and '530 Login incorrect' in data
    partition by destIp
    window.externalTimeBatch('timestamp', $time_threshold)
    aggregate count() as no_of_packets
    filter no_of_packets >= $count_threshold
    select srcIp, destIp, no_of_packets, timestamp
insert into AttackStream;
```

## HTTP Slow Header Attack

HTTP Slow Header attack is a Denial of Service(DOS) attack in which a victim server is compromized by sending too many HTTP incomplete requests with random `Keep-Alive` time. For more details, read: [How Secure are Web Servers? An Empirical Study of Slow HTTP DoS Attacks and Detection](https://ieeexplore.ieee.org/document/7784605/).

```java
@app(name='SlowHeaderDetector', version='1.0.0')
def stream PacketStream;
def stream AttackStream;

def variable time_threshold = time.sec(1);
def variable count_threshold = 998;

from PacketStream
    filter 'http' == app_protocol and destPort == 80 and '\r\n\r\n' in data and 'Keep-Alive: \\d+' in data
    partition by destIp
    window.externalTimeBatch('timestamp', $time_threshold)
    aggregate count() as no_of_packets
    filter no_of_packets >= $count_threshold
    select srcIp, destIp, no_of_packets, timestamp
insert into AttackStream;
```

## Port Scanning

Even though Port Scanning is a common technique used by attackers, it is hard to fit all types of port scans into a single CEP rule. The following rule is developed to detect `nmap -sS` port scan. For more details, please visit [Port Scanning Techniques](https://nmap.org/book/man-port-scanning-techniques.html).

```java

@app(name='PortScanDetector', version='1.0.0')
def stream PacketStream;
def stream AttackStream;

@config(trainable=true, minimum=100, maximum=60000, step=-1)
def variable time_threshold = 761;

@config(trainable=true, minimum=3, maximum=1000, step=1)
def variable count_threshold = 3;

from PacketStream
    filter syn == true and ack == false
    partition by srcIp, destIp
    window.unique:externalTimeBatch('destPort', 'timestamp', $time_threshold)
    aggregate count() as no_of_packets
    filter no_of_packets >= $count_threshold
    select srcIp, destIp, no_of_packets, timestamp
insert into AttackStream;
```