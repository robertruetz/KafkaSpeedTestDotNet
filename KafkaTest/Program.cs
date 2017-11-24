using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace KafkaTest
{
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

            // Parse command line args
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
            
            var serializer = new AvroSerializer(User._SCHEMA); 
            var config = new Dictionary<string, object>()
                {
                    {"bootstrap.servers", "ruetz-brkr1.dev-ruetz.service.consul:10086"}
                };
            var kafka = new Kafka(config, config);

            if (multithreaded)
            {
                //Do it with threads
            }
            else
            {
                globalSw.Start();
                var users = CreateUserList(num, GlobalLogger, ref taskSw);
                var msgs = AvroSerializeMessages(users, serializer, GlobalLogger, ref taskSw);                
                SendMessagesToKafka(kafka, msgs, topic, GlobalLogger, ref taskSw);
                var received = ReadMessagesFromKafka(kafka, topic, num, GlobalLogger, ref taskSw);
                var deserialized = AvroDeserializeMessages(received, serializer, GlobalLogger, ref taskSw);
                globalSw.Stop();
                GlobalLogger.WriteLogInfo($"Script run time: {globalSw.ElapsedMilliseconds} ms.");
            }


        }

        static List<User> AvroDeserializeMessages(List<byte[]> msgs, AvroSerializer serializer, Logger log, ref Stopwatch sw)
        {
            sw.Reset();
            Print("Deserializing received messages.");
            sw.Start();
            var deserialized = new List<User>();
            foreach (var msg in msgs)
            {
                deserialized.Add(serializer.Deserialize<User>(msg));
            }
            sw.Stop();
            log.WriteLogDebug($"{deserialized.Count} messages deserialized in {sw.ElapsedMilliseconds} ms.");
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

        static void Print(string msg)
        {
            Console.WriteLine(msg);
        }
    }
}
