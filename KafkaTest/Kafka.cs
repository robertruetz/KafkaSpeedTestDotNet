using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System.Threading;

namespace KafkaTest
{
    public class DoNothingSerializer: ISerializer<byte[]>, IDeserializer<byte[]>
    {
        public DoNothingSerializer()
        {
            
        }

        public byte[] Serialize(byte[] obj)
        {
            return obj;
        }

        public byte[] Deserialize(byte[] obj)
        {
            return obj;
        }
    }

    class Kafka : IDisposable
    {
        public Dictionary<string, object> ConsumerConfig;
        public Dictionary<string, object> ProducerConfig;
        public Producer<Null, byte[]> Prod;
        public Consumer<Null, byte[]> Cons;
        public ThreadSafeQueue<byte[]> ReceivedBytes;
        public ThreadSafeQueue<byte[]> ToProduceBytes;
        public ThreadSafeQueue<User> UserQueue;
        public Logger Log;
        public bool ConsumerAssigned = false;
        public int MessagesRead = 0;

        public Kafka(Dictionary<string, object> consumerConfig, Dictionary<string, object> producerConfig, Logger log)
        {
            ConsumerConfig = consumerConfig;
            ProducerConfig = producerConfig;
            Log = log;
            Prod = new Producer<Null, byte[]>(ProducerConfig, new NullSerializer(), new DoNothingSerializer());
            Cons = new Consumer<Null, byte[]>(ConsumerConfig, new NullDeserializer(), new DoNothingSerializer());
            Cons.OnMessage += (_, message) =>
            {
                ReceivedBytes.Enqueue(message.Value);
                MessagesRead++;
            };
            Cons.OnPartitionsAssigned += (_, partitions) =>
            {
                var assignedParts = string.Join(", ", partitions);
                if (!string.IsNullOrEmpty(assignedParts))
                {
                    Cons.Assign(partitions);
                    ConsumerAssigned = true;
                    Log.WriteLogInfo($"Partitions assigned: {assignedParts}");
                }
                else
                {
                    Log.WriteLogInfo($"Partitions was null or empty. Continuing to poll.");
                }
            };
            Cons.OnError += (_, error) =>
            {
                Log.WriteLogError(error.ToString());
            };
            Cons.OnConsumeError += (_, error) =>
            {
                Log.WriteLogError(error.ToString());
            };
            ReceivedBytes = new ThreadSafeQueue<byte[]>();
            ToProduceBytes = new ThreadSafeQueue<byte[]>();
            UserQueue = new ThreadSafeQueue<User>();
        }

        public Consumer<Null, byte[]> GetConsumer()
        {
            // Add extra config in an effort to make this thing consume messages. Grrrrr.
            // ConsumerConfig["group.id"] = Guid.NewGuid();
            ConsumerConfig["group.id"] = "dotnet_speed_test";
            ConsumerConfig["enable.auto.commit"] = true;
            ConsumerConfig["default.topic.config"] = new Dictionary<string, object>()
            {
                { "auto.offset.reset", "smallest" }
            };
            return new Consumer<Null, byte[]>(ConsumerConfig, new NullDeserializer(), new DoNothingSerializer());
        }

        public Producer<Null, byte[]> GetProducer()
        {
            return new Producer<Null, byte[]>(ProducerConfig, new NullSerializer(), new DoNothingSerializer());
        }

        public void ProduceMessages(List<byte[]> messages, string topic)
        {
            List<Task<Message<Null, byte[]>>> taskList = new List<Task<Message<Null, byte[]>>>();
            foreach (byte[] message in messages)
            {
                taskList.Add(Prod.ProduceAsync(topic, null, message));//TODO configurable message send timeout    
            }
            var messagesSent = 0;
            var sw = new Stopwatch();
            sw.Start();
            while (taskList.Count > 0)
            {
                for (int i = 0; i < taskList.Count; i++)
                {
                    if (taskList[i].Status == TaskStatus.RanToCompletion)
                    {
                        messagesSent++;
                        taskList.Remove(taskList[i]);
                    }
                }
                if (sw.ElapsedMilliseconds > 30000)
                {
                    Log.WriteLogError("Waited longer than 30 seconds for a kafka message to send");
                    throw new TimeoutException("Waited longer than 30 seconds for a kafka message to send");
                }
                Thread.Sleep(100);
            }
            sw.Stop();
            Prod.Flush(0);
        }

        public void SendMessages(List<byte[]> messages, string topic)
        {
            using (var prod = new Producer<Null, byte[]>(ProducerConfig, new NullSerializer(), new DoNothingSerializer()))
            {
                List<Task<Message<Null, byte[]>>> taskList = new List<Task<Message<Null, byte[]>>>();
                foreach (byte[] message in messages)
                {
                    taskList.Add(prod.ProduceAsync(topic, null, message));//TODO configurable message send timeout    
                }
                var messagesSent = 0;
                var sw = new Stopwatch();
                sw.Start();
                while (taskList.Count > 0)
                {
                    for (int i = 0; i < taskList.Count; i++)
                    {
                        if (taskList[i].Status == TaskStatus.RanToCompletion)
                        {
                            messagesSent++;
                            taskList.Remove(taskList[i]);
                        }
                    }
                    if (sw.ElapsedMilliseconds > 30000)
                    {
                        Log.WriteLogError("Waited longer than 30 seconds for a kafka message to send");
                        throw new TimeoutException("Waited longer than 30 seconds for a kafka message to send");
                    }
                    System.Threading.Thread.Sleep(50);
                }
                sw.Stop();
                prod.Flush(0);     // Shouln't need this as we're explicitly waiting for the tasks to complete. 
            }
        }

        public List<byte[]> ReadMessages(string topic, int numExpected)
        {
            var keepGoing = true;
            var output = new List<byte[]>();

            // Add extra config in an effort to make this thing consume messages. Grrrrr.
            // ConsumerConfig["group.id"] = Guid.NewGuid();
            ConsumerConfig["group.id"] = "dotnet_speed_test";
            ConsumerConfig["enable.auto.commit"] = true;
            ConsumerConfig["default.topic.config"] = new Dictionary<string, object>()
            {
                { "auto.offset.reset", "smallest" }
            };

            using (var consumer = new Consumer<Null, byte[]>(ConsumerConfig, new NullDeserializer(), new DoNothingSerializer()))
            {
                consumer.OnMessage += (_, message) =>
                {
                    if (message.Topic == topic)
                    {
                        output.Add(message.Value);
                    }
                };
                consumer.OnPartitionEOF += (_, end) =>
                {
                    Log.WriteLogInfo($"End of topic partition reached.");
                    keepGoing = false;
                };
                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    var assignedParts = string.Join(", ", partitions);
                    if (!string.IsNullOrEmpty(assignedParts))
                    {
                        consumer.Assign(partitions);
                        Log.WriteLogInfo($"Partitions assigned: {assignedParts}");
                    }
                    else
                    {
                        Log.WriteLogInfo($"Partitions was null or empty. Continuing to poll.");
                    }
                };
                consumer.OnError += (_, error) =>
                {
                    Log.WriteLogError(error.ToString());
                };
                consumer.OnConsumeError += (_, error) =>
                {
                    Log.WriteLogError(error.ToString());
                };

                consumer.Subscribe(new List<string> { topic });
                while (output.Count < numExpected)
                {
                    consumer.Poll(100);
                    Thread.Sleep(100);
                    if (!keepGoing)
                    {
                        break;
                    }
                }
            }
            return output;
        }

        public void Dispose()
        {
            Log.WriteLogInfo("Disposing of Kafka.");
            this.Prod.Dispose();
            this.Cons.Dispose();
            Prod = null;
            Cons = null;
        }
    }
}
