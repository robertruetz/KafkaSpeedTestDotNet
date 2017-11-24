using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace KafkaTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var globalSw = new Stopwatch();
            var taskSw = new Stopwatch();
            var topic = "speed_test_dotnet";
            globalSw.Start();

            int num = 1000;
            if (args.Length > 0)
            {
                if (!int.TryParse(args[0], out num))
                {
                    Console.WriteLine($"Argument {args[0]} could not be parsed to an Int.");
                    Environment.Exit(2);
                }
            }

            // Create randomized Users
            Print($"Creating {num} messages.");
            var userCreateSw = new Stopwatch();
            userCreateSw.Start();
            var users = CreateUserList(num);
            userCreateSw.Stop();
            Console.WriteLine($"User creation took {userCreateSw.ElapsedMilliseconds} ms.");

            var serializer = new AvroSerializer(User._SCHEMA);
            List<byte[]> msgs = new List<byte[]>();

            // Avro Serialize messages.
            Print("Avro serializing messages");
            var serialSw = new Stopwatch();
            serialSw.Start();
            foreach (var user in users)
            {
                msgs.Add(serializer.Serialize<User>(user));
            }
            serialSw.Stop();
            Console.WriteLine($"Avro Serialization took {serialSw.ElapsedMilliseconds} ms.");

            // Produce messages to Kafka
            Print($"Producing {msgs.Count} messages to Kafka.");
            var config = new Dictionary<string, object>()
            {
                {"bootstrap.servers", "ruetz-brkr1.dev-ruetz.service.consul:10086"}
            };
            var kafka = new Kafka(config, config);
            var sw = new Stopwatch();
            sw.Start();
            kafka.SendMessages(msgs, topic);
            sw.Stop();
            Console.WriteLine($"Messages produced in {sw.ElapsedMilliseconds} ms.");

            // Consume messages from Kafka
            sw.Reset();
            Print($"Consuming messages from Kafka.");
            sw.Start();
            var received = kafka.ReadMessages(topic, msgs.Count);
            sw.Stop();
            Console.WriteLine($"{received.Count} messages consumed in {sw.ElapsedMilliseconds} ms.");

            // Deserialize messages
            sw.Reset();
            sw.Start();
            Print("Deserializing received messages.");
            var deserialized = new List<User>();
            foreach(var msg in received)
            {
                deserialized.Add(serializer.Deserialize<User>(msg));
            }
            sw.Stop();
            Print($"{deserialized.Count} messages deserialized in {sw.ElapsedMilliseconds} ms.");

            globalSw.Stop();
            Console.WriteLine($"Script run time: {globalSw.ElapsedMilliseconds} ms.");
        }

        static List<User> CreateUserList(int num)
        {
            var rand = new Random();
            var output = new List<User>();
            for (int x = 0; x < num; x++)
            {
                output.Add(new User(GetRandomString(10), rand.Next(100), GetRandomColor()));
            }
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
