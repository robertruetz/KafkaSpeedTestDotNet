using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;

namespace KafkaTest
{
    class AvroSerializerTask : ThreadedTaskWorker
    {
        public ThreadSafeQueue<User> UsersQueue;
        public ThreadSafeQueue<byte[]> SerializedUsersQueue;
        public Logger Log;
        public AvroSerializer Serializer = new AvroSerializer(User._SCHEMA);
        public int NumExpected;

        public AvroSerializerTask(ILogger logger, CountdownEvent countDownEvent, ThreadSafeQueue<User> usersQueue, ThreadSafeQueue<byte[]> serializedUsersQueue, int numExpected)
            : base(logger, countDownEvent)
        {
            UsersQueue = usersQueue;
            SerializedUsersQueue = serializedUsersQueue;
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
                    SerializedUsersQueue.Enqueue(Serializer.Serialize<User>(user));
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
        public ThreadSafeQueue<byte[]> SerializedUsersQueue;
        public Kafka KafkaConnection;
        public Logger Log;
        public string Topic;
        public int BatchSize;
        public int NumExpected;

        public KafkaWriteTask(ILogger logger, CountdownEvent countdownEvent, ThreadSafeQueue<byte[]> serializedUsersQueue, Kafka kafka, string topic, int batchSize, int numExpected)
            : base(logger, countdownEvent)
        {
            SerializedUsersQueue = serializedUsersQueue;
            Log = logger as Logger;
            KafkaConnection = kafka;
            Topic = topic;
            BatchSize = batchSize;
            NumExpected = numExpected;
        }

        public override void Run()
        {
            _startSignal.WaitOne();
            Log.WriteLogInfo("KafkaWriteTask now running.");
            var sw = new Stopwatch();
            var numDone = 0;
            sw.Start();
            while(_running)
            {
                var msgs = new List<byte[]>();
                while (SerializedUsersQueue.TryDequeue(out byte[] msg))
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
                    KafkaConnection.SendMessages(msgs, Topic);
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
        public ThreadSafeQueue<byte[]> ReceivedQueue;
        public Logger Log;
        public Kafka KafkaConnection;
        public string Topic;
        public int NumExpected;

        public KafkaReadThread(ILogger logger, CountdownEvent countDownEvent, ThreadSafeQueue<byte[]> readMessages, Kafka kafka, string topic, int numExpected) 
            : base(logger, countDownEvent)
        {
            ReceivedQueue = readMessages;
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
            using (var consumer = KafkaConnection.GetConsumer())
            {
                consumer.OnMessage += (_, message) =>
                {
                    if (message.Topic == Topic)
                    {
                        ReceivedQueue.Enqueue(message.Value);
                        numRead++;
                    }
                };
                consumer.OnPartitionEOF += (_, end) =>
                {
                    Log.WriteLogInfo($"End of topic partition reached.");
                    _running = false;
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

                consumer.Subscribe(new List<string> { Topic });
                while (_running)
                {
                    consumer.Poll(100);
                    if (numRead >= NumExpected)
                    {
                        _running = false;
                    }
                    _startSignal.WaitOne();
                    Thread.Sleep(100);
                }
                sw.Stop();
                Log.WriteLogInfo($"{numRead} messages read in {sw.ElapsedMilliseconds} ms.");
                _countdownEvent.Signal();
            }
        }
    }

    class AvroDeserializeTask : ThreadedTaskWorker
    {
        public ThreadSafeQueue<byte[]> ReceivedQueue;
        public ThreadSafeQueue<User> DeserializedQueue;
        public Logger Log;
        public AvroSerializer Serializer;
        public int NumExpected;

        public AvroDeserializeTask(ILogger logger, CountdownEvent countDownEvent, ThreadSafeQueue<byte[]> readQueue, ThreadSafeQueue<User> deserializedQueue, AvroSerializer serializer, int numExpected) 
            : base(logger, countDownEvent)
        {
            Log = logger as Logger;
            ReceivedQueue = readQueue;
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
                if (ReceivedQueue.TryDequeue(out byte[] data))
                {
                    DeserializedQueue.Enqueue(Serializer.Deserialize<User>(data));
                    numDone++;
                }
                if (numDone >= NumExpected)
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
            var globalSw = new Stopwatch();
            var taskSw = new Stopwatch();
            var topic = "speed_test_dotnet";
            var multithreaded = false;
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
            var config = new Dictionary<string, object>()
                {
                    {"bootstrap.servers", "ruetz-brkr1.dev-ruetz.service.consul:10086"}
                };
            var kafka = new Kafka(config, config, GlobalLogger);

            globalSw.Start();
            if (multithreaded)
            {
                var userQueue = new ThreadSafeQueue<User>();
                var serializedQueue = new ThreadSafeQueue<byte[]>();
                var receivedQueue = new ThreadSafeQueue<byte[]>();
                var deserializedQueue = new ThreadSafeQueue<User>();

                var countDown = new CountdownEvent(1);
                var taskList = new List<ThreadedTaskWorker>()
                {
                    new AvroSerializerTask(GlobalLogger, countDown, userQueue, serializedQueue, num),
                    new KafkaWriteTask(GlobalLogger, countDown, serializedQueue, kafka, topic, 100, num),
                    new KafkaReadThread(GlobalLogger, countDown, receivedQueue, kafka, topic, num),
                    new AvroDeserializeTask(GlobalLogger, countDown, receivedQueue, deserializedQueue, serializer, num)
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
            sw.Start();
            var received = kafka.ReadMessages(topic, numExpected);
            sw.Stop();
            log.WriteLogInfo($"{received.Count} messages consumed in {sw.ElapsedMilliseconds} ms.");
            return received;
        }

        static void SendMessagesToKafka(Kafka kafka, List<byte[]> msgs, string topic, Logger log, ref Stopwatch sw)
        {
            sw.Reset();
            log.WriteLogInfo($"Producing {msgs.Count} messages to Kafka.");            
            sw.Start();
            kafka.SendMessages(msgs, topic);
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
