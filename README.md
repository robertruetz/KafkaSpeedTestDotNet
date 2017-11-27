# KafkaSpeedTestDotNet
.Net 4.5.1 project for measuring speed of a Kafka producer/consumer at variable message loads

USAGE: ```KafkaTest.exe [optional: mt] numberOfMessages```

```KafkaTest.exe 1000``` : will run synchronously and produce/consume 1000 messages
  
```KafkaTest.exe mt 1000``` : will run asynchronously and produce/consume 1000 messages
  
