# KafkaSpeedTestDotNet
.Net 4.5.1 project for measuring speed of a Kafka producer/consumer at variable message loads

Must set environment variable: ```SET KAFKA_BROKERS=100.0.0.0:10080```

USAGE: ```KafkaTest.exe [optional: mt] numberOfMessages```

```KafkaTest.exe 1000``` : will run synchronously and produce/consume 1000 messages
  
```KafkaTest.exe mt 1000``` : will run asynchronously and produce/consume 1000 messages
  
