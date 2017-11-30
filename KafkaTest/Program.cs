using System;
using System.IO;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System.Threading.Tasks;
using System.Text;

namespace KafkaTest
{
    class AvroSerializerTask : ThreadedTaskWorker
    {
        public ThreadSafeQueue<User> UsersQueue;
        public Kafka KafkaConnection;
        public Logger Log;
        public AvroSerializer Serializer = new AvroSerializer(User._SCHEMA);
        public int NumExpected;

        public AvroSerializerTask(ILogger logger, CountdownEvent countDownEvent, ThreadSafeQueue<User> usersQueue, Kafka kafka, int numExpected)
            : base(logger, countDownEvent)
        {
            UsersQueue = usersQueue;
            KafkaConnection = kafka;
            NumExpected = numExpected;
            Log = logger as Logger;
        }

        public override void Run()
        {
            _startSignal.WaitOne();
            var sw = new Stopwatch();
            var numDone = 0;
            sw.Start();
            while (_running)
            {
                while (UsersQueue.TryDequeue(out User user))
                {
                    KafkaConnection.ToProduceBytes.Enqueue(Serializer.Serialize<User>(user));
                    numDone++;
                    if (numDone >= NumExpected)
                    {
                        _running = false;
                    }
                }
                _startSignal.WaitOne();
                Thread.Sleep(100);
            }
            sw.Stop();
            Log.WriteLogInfo($"{numDone} messages serialized in {sw.ElapsedMilliseconds} ms.");
            _countdownEvent.Signal();
        }
    }

    class KafkaWriteTask : ThreadedTaskWorker
    {
        public Kafka KafkaConnection;
        public Logger Log;
        public string Topic;
        public int BatchSize;
        public int NumExpected;

        public KafkaWriteTask(ILogger logger, CountdownEvent countdownEvent, Kafka kafka, string topic, int batchSize, int numExpected)
            : base(logger, countdownEvent)
        {
            Log = logger as Logger;
            KafkaConnection = kafka;
            Topic = topic;
            BatchSize = batchSize;
            NumExpected = numExpected;
        }

        public override void Run()
        {
            _startSignal.WaitOne();
            var sw = new Stopwatch();
            var numDone = 0;
            sw.Start();
            while(_running)
            {
                var msgs = new List<byte[]>();
                while (KafkaConnection.ToProduceBytes.TryDequeue(out byte[] msg))
                {
                    msgs.Add(msg);
                    numDone++;
                    if (numDone >= NumExpected)
                    {
                        _running = false;
                    }
                    if (msgs.Count >= BatchSize)
                    {
                        break;
                    }
                }
                if (msgs.Count > 0)
                {
                    KafkaConnection.ProduceMessages(msgs, Topic);
                    msgs.Clear();
                }
                _startSignal.WaitOne();
                Thread.Sleep(100);
            }
            sw.Stop();
            Log.WriteLogInfo($"{numDone} messages written to Kafka in {sw.ElapsedMilliseconds} ms.");
            _countdownEvent.Signal();
        }
    }

    class KafkaReadThread : ThreadedTaskWorker
    {
        public Logger Log;
        public Kafka KafkaConnection;
        public string Topic;
        public int NumExpected;

        public KafkaReadThread(ILogger logger, CountdownEvent countDownEvent, Kafka kafka, string topic, int numExpected) 
            : base(logger, countDownEvent)
        {
            KafkaConnection = kafka;
            Topic = topic;
            NumExpected = numExpected;
            Log = logger as Logger;
        }

        public override void Run()
        {
            var sw = new Stopwatch();
            var numRead = 0;
            _startSignal.WaitOne();
            sw.Start();

            while (_running)
            {
                KafkaConnection.Cons.Poll(100);
                if (NumExpected == 0)
                {
                    // We're just warming up. Keep looping.
                    continue;
                }
                else if (KafkaConnection.MessagesRead >= NumExpected)
                {
                    _running = false;
                }
                _startSignal.WaitOne();
                Thread.Sleep(100);
            }
            sw.Stop();
            Log.WriteLogInfo($"{KafkaConnection.MessagesRead} messages read in {sw.ElapsedMilliseconds} ms.");
            _countdownEvent.Signal();
        }
    }

    class AvroDeserializeTask : ThreadedTaskWorker
    {
        public ThreadSafeQueue<User> DeserializedQueue;
        public Kafka KafkaConnection;
        public Logger Log;
        public AvroSerializer Serializer;
        public int NumExpected;

        public AvroDeserializeTask(ILogger logger, CountdownEvent countDownEvent, Kafka kafka, ThreadSafeQueue<User> deserializedQueue, AvroSerializer serializer, int numExpected) 
            : base(logger, countDownEvent)
        {
            Log = logger as Logger;
            KafkaConnection = kafka;
            DeserializedQueue = deserializedQueue;
            Serializer = serializer;
            NumExpected = numExpected;
        }

        public override void Run()
        {
            _startSignal.WaitOne();
            var sw = new Stopwatch();
            var numDone = 0;
            sw.Start();
            while(_running)
            {
                if (KafkaConnection.ReceivedBytes.TryDequeue(out byte[] data))
                {
                    DeserializedQueue.Enqueue(Serializer.Deserialize<User>(data));
                    numDone++;
                }
                if (DeserializedQueue.Count() >= NumExpected)
                {
                    _running = false;
                }
                _startSignal.WaitOne();
                Thread.Sleep(100);
            }
            sw.Stop();
            Log.WriteLogInfo($"{numDone} messages deserialized in {sw.ElapsedMilliseconds} ms.");
            _countdownEvent.Signal();
        }
    }

    class Program
    {
        public static Logger GlobalLogger = new Logger("KafkaSpeedTest");

        static void PrintUsage()
        {
            Console.WriteLine("KafkaTest.exe [optional: --mt] numOfMessages");
            Console.WriteLine("\t--mt == Run multi-threaded");
            Console.WriteLine("\tnumOfMessages == Integer number of messages you want to send/receive.");
            Console.WriteLine("\n\tExample: KafkaTest.exe --mt 1000");
        }

        static void Main(string[] args)
        {
            System.Threading.ThreadPool.SetMinThreads(50, 30);
            var globalSw = new Stopwatch();
            var taskSw = new Stopwatch();
            var topic = "speed_test_dotnet";
            var multithreaded = false;
            var load = false;
            int num = 1000;

            #region parseCommandLineArgs
            if (args.Length > 0)
            {
                foreach (var arg in args)
                {
                    if (arg.Contains("mt"))
                    {
                        multithreaded = true;
                    }
                    else if (arg.Contains("load"))
                    {
                        load = true;
                    }
                    else if (!int.TryParse(arg, out num))
                    {
                        GlobalLogger.WriteLogError($"Argument {arg} is not valid.");
                        PrintUsage();
                        Environment.Exit(2);
                    }
                }
            }
            #endregion parseCommandLineArgs
            
            var serializer = new AvroSerializer(User._SCHEMA);
            var brokers = System.Environment.GetEnvironmentVariable("KAFKA_BROKERS");
#if DEBUG
            brokers = "104.198.16.33:10086";
#endif

            if (string.IsNullOrEmpty(brokers))
            {
                GlobalLogger.WriteLogError("KAFKA_BROKERS env var not set.");
                Environment.Exit(2);
            }
            var producerConfig = new Dictionary<string, object>()
            {
                {"bootstrap.servers", brokers}
            };
            var consumerConfig = new Dictionary<string, object>()
            {
                // Add extra config in an effort to make this thing consume messages. Grrrrr.
                //{ "group.id", Guid.NewGuid() },
                {"bootstrap.servers", brokers},
                { "group.id", "dotnet_speed_test" },
                { "enable.auto.commit", true },
                { "default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", "smallest" }
                    }
                }
            };

            var prodSw = new Stopwatch();
            if (load)
            {
                topic = "load_test_dotnet";
                GlobalLogger.WriteLogInfo($"Producing {num} messages to topic {topic}");
                var count = 0;
                prodSw.Start();
                Task.Run(() =>
                   {
                       using (var producer = new Producer(producerConfig))
                       {
                           while (count < num)
                           {
                               var payload = DateTime.UtcNow.ToLongDateString();
                               producer.ProduceAsync(topic, null, Encoding.ASCII.GetBytes(payload));
                               count++;
                           }
                       }
                   }).Wait();
                
                //using (var producer = new Producer<Null, int>(producerConfig, new Confluent.Kafka.Serialization.NullSerializer(), new IntSerializer()))
                //{
                //    prodSw.Start();
                //    var payload = 0;
                //    var taskList = new List<Task<Message<Null, int>>>();
                //    while (payload < num)
                //    {
                //        taskList.Add(producer.ProduceAsync(topic, null, payload));
                //        payload++;
                //    }
                //    while (taskList.Count > 0)
                //    {
                //        for (int i = 0; i < taskList.Count; i++)
                //        {
                //            if (taskList[i].Status == TaskStatus.RanToCompletion)
                //            {
                //                taskList.Remove(taskList[i]);
                //            }
                //        }
                //        if (prodSw.ElapsedMilliseconds > 30000)
                //        {
                //            GlobalLogger.WriteLogError("Waited longer than 30 seconds for a kafka message to send");
                //            throw new TimeoutException("Waited longer than 30 seconds for a kafka message to send");
                //        }
                //        Thread.Sleep(100);
                //    }
                    //producer.Flush(0);
                    prodSw.Stop();
                    GlobalLogger.WriteLogInfo($"Done producing.");
                    var msgPerSec = prodSw.ElapsedMilliseconds / count;
                    GlobalLogger.WriteLogInfo($"{count} messages produced in {prodSw.ElapsedMilliseconds} ms. Rate: {msgPerSec} msgs/ms.");

                    using (var consumer = new Consumer<Null, int>(consumerConfig, new NullDeserializer(), new IntDeserializer()))
                    {
                        var keepGoing = true;
                        var assigned = false;
                        var messagesRead = 0;
                    
                        consumer.OnMessage += (_, message) =>
                        {
                            // GlobalLogger.WriteLogDebug($"Message received. That makes {messagesRead}.");
                            messagesRead++;
                        };
                        consumer.OnPartitionEOF += (_, partition) =>
                        {
                            keepGoing = false;
                        };
                        consumer.OnPartitionsAssigned += (_, partitions) =>
                        {
                            GlobalLogger.WriteLogInfo($"Assigned partitions: [{string.Join(", ", partitions)}]");
                            List<TopicPartitionOffset> assignments = new List<TopicPartitionOffset>();
                            foreach (var part in partitions)
                            {
                                var offsets = consumer.QueryWatermarkOffsets(part, TimeSpan.FromMilliseconds(1000));
                                GlobalLogger.WriteLogDebug($"offsets -- High: {offsets.High}, Low: {offsets.Low}");
                                assignments.Add(new TopicPartitionOffset(part, offsets.Low));
                            }
                            consumer.Assign(assignments.ToArray());
                            //consumer.Assign(partitions);
                            assigned = true;
                        };
                        consumer.OnError += (_, error) =>
                        {
                            GlobalLogger.WriteLogError(error.Reason);
                        };
                        consumer.OnConsumeError += (_, error) =>
                        {
                            GlobalLogger.WriteLogError(error.Error.Reason);
                        };
                    
                        consumer.Subscribe(new List<string> { topic });
                        GlobalLogger.WriteLogInfo("Waiting for topic assignment.");
                        while (!assigned)
                        {
                            consumer.Poll(100);
                            Thread.Sleep(100);
                        }
                        Thread.Sleep(1000);
                    
                        prodSw.Reset();
                        prodSw.Start();
                        while(keepGoing)
                        {
                            consumer.Poll(100);
                            Thread.Sleep(100);
                        }
                        prodSw.Stop();
                        GlobalLogger.WriteLogInfo($"{messagesRead} messages consumed in {prodSw.ElapsedMilliseconds} ms. Rate {prodSw.ElapsedMilliseconds / messagesRead} msgs/ms.");
                    }
                    GlobalLogger.WriteLogInfo("Done.");
                    return;
            }

            using (var kafka = new Kafka(consumerConfig, producerConfig, GlobalLogger))
            {
                kafka.Cons.Subscribe(new List<string> { topic });
                while (!kafka.ConsumerAssigned)
                {
                    kafka.Cons.Poll(100);
                    Thread.Sleep(100);
                }

                #region primeThePump
                GlobalLogger.WriteLogInfo("Priming Kafka.");
                var user = new User("PRIMING", 1, "PEUCE");
                var serialized = AvroSerializeMessages(new List<User>() { user }, serializer, GlobalLogger, ref taskSw);
                kafka.ProduceMessages(serialized, topic);
                while (kafka.ReceivedBytes.Count() < 1)
                {
                    kafka.Cons.Poll(100);
                    Thread.Sleep(100);
                }
                kafka.ReceivedBytes.ClearQueue();
                kafka.MessagesRead = 0;
                GlobalLogger.WriteLogInfo("Kafka primed.");
                #endregion primeThePump

                globalSw.Start();
                GlobalLogger.WriteLogInfo($"Starting test pass with {num} messages.");
                if (multithreaded)
                {
                    var userQueue = new ThreadSafeQueue<User>();
                    var serializedQueue = new ThreadSafeQueue<byte[]>();
                    var deserializedQueue = new ThreadSafeQueue<User>();

                    var countDown = new CountdownEvent(1);

                    var taskList = new List<ThreadedTaskWorker>()
                    {
                        new AvroSerializerTask(GlobalLogger, countDown, userQueue, kafka, num),
                        new KafkaWriteTask(GlobalLogger, countDown, kafka, topic, 100, num),
                        new KafkaReadThread(GlobalLogger, countDown, kafka, topic, num),
                        new AvroDeserializeTask(GlobalLogger, countDown, kafka, deserializedQueue, serializer, num)
                    };
                    var threads = new List<Thread>();
                    Console.CancelKeyPress += (sender, eventArgs) =>
                    {
                        GlobalLogger.WriteLogInfo("Stopping tasks.");
                        foreach (var task in taskList)
                        {
                            task.Stop();
                        }
                        foreach (var thr in threads)
                        {
                            thr.Join(2000);
                            if (thr.ThreadState == System.Threading.ThreadState.Running)
                            {
                                thr.Abort();
                            }
                        }
                    };
                    foreach (var task in taskList)
                    {
                        countDown.AddCount();
                        var nThread = new Thread(task.Run);
                        nThread.IsBackground = true;
                        threads.Add(nThread);
                        nThread.Start();
                        task.Start();
                    }
                    countDown.Signal();
                    var rand = new Random();

                    for (int i = 0; i < num; i++)
                    {
                        userQueue.Enqueue(new User(GetRandomString(10), rand.Next(100), GetRandomColor()));
                    }
                    countDown.Wait();
                }
                else
                {
                    var users = CreateUserList(num, GlobalLogger, ref taskSw);
                    var msgs = AvroSerializeMessages(users, serializer, GlobalLogger, ref taskSw);
                    SendMessagesToKafka(kafka, msgs, topic, GlobalLogger, ref taskSw);
                    var received = ReadMessagesFromKafka(kafka, topic, num, GlobalLogger, ref taskSw);
                    var deserialized = AvroDeserializeMessages(received, serializer, GlobalLogger, ref taskSw);
                } 
            }
            globalSw.Stop();
            GlobalLogger.WriteLogInfo($"Script run time: {globalSw.ElapsedMilliseconds} ms.");
        }

        static List<User> AvroDeserializeMessages(List<byte[]> msgs, AvroSerializer serializer, Logger log, ref Stopwatch sw)
        {
            sw.Reset();
            log.WriteLogInfo("Deserializing received messages.");
            sw.Start();
            var deserialized = new List<User>();
            foreach (var msg in msgs)
            {
                deserialized.Add(serializer.Deserialize<User>(msg));
            }
            sw.Stop();
            log.WriteLogInfo($"{deserialized.Count} messages deserialized in {sw.ElapsedMilliseconds} ms.");
            return deserialized;
        }

        static List<byte[]> ReadMessagesFromKafka(Kafka kafka, string topic, int numExpected, Logger log, ref Stopwatch sw)
        {
            sw.Reset();
            log.WriteLogInfo($"Consuming messages from Kafka.");
            var received = new List<byte[]>();
            kafka.Cons.OnMessage += (_, message) =>
            {
                received.Add(message.Value);
            };
            sw.Start();
            while(received.Count < numExpected)
            {
                kafka.Cons.Poll(100);
                Thread.Sleep(100);
            }
            sw.Stop();
            log.WriteLogInfo($"{received.Count} messages consumed in {sw.ElapsedMilliseconds} ms.");
            return received;
        }

        static void SendMessagesToKafka(Kafka kafka, List<byte[]> msgs, string topic, Logger log, ref Stopwatch sw)
        {
            sw.Reset();
            log.WriteLogInfo($"Producing {msgs.Count} messages to Kafka.");            
            sw.Start();
            kafka.ProduceMessages(msgs, topic);
            sw.Stop();
            log.WriteLogInfo($"Messages produced in {sw.ElapsedMilliseconds} ms.");
        }
        
        static List<byte[]> AvroSerializeMessages(List<User> users, AvroSerializer serializer, Logger log, ref Stopwatch sw)
        {
            sw.Reset();
            log.WriteLogInfo("Avro serializing messages");
            sw.Start();
            List<byte[]> msgs = new List<byte[]>();
            foreach (var user in users)
            {
                msgs.Add(serializer.Serialize<User>(user));
            }
            sw.Stop();
            log.WriteLogInfo($"Avro Serialization took {sw.ElapsedMilliseconds} ms.");
            return msgs;
        }

        static List<User> CreateUserList(int num, Logger log, ref Stopwatch sw)
        {
            sw.Reset();
            log.WriteLogInfo($"Creating {num} messages.");
            sw.Start();
            var rand = new Random();
            var output = new List<User>();
            for (int x = 0; x < num; x++)
            {
                output.Add(new User(GetRandomString(10), rand.Next(100), GetRandomColor()));
            }
            sw.Stop();
            log.WriteLogInfo($"User creation took {sw.ElapsedMilliseconds} ms.");
            return output;
        }

        static string GetRandomColor()
        {
            var rand = new Random();
            var colors = new List<string> { "Red", "Blue", "Green", "Yellow", "Orange", "Purple" };
            return colors[rand.Next(colors.Count())];
        }

        static string GetRandomString(int length)
        {
            var stringChars = new char[length];
            var chars = "abcdefghijklmnopqrstuvwxyz";
            var random = new Random();
            for (int i = 0; i < length; i++)
            {
                stringChars[i] = chars[random.Next(chars.Length)];
            }
            return new string(stringChars);
        }
    }
}
