using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Avro;
using Avro.IO;
using Avro.File;
using Avro.Generic;
using Avro.Specific;

namespace KafkaTest
{
    class AvroSerializer
    {
        public Avro.Schema Schema;

        public AvroSerializer(Avro.Schema schema)
        {
            Schema = schema;
        }

        public AvroSerializer(string schema, bool isPath)
        {
            if (isPath)
            {
                var fullPath = Path.GetFullPath(schema);
                if (File.Exists(fullPath))
                {
                    Schema = Avro.RecordSchema.Parse(File.ReadAllText(fullPath));
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(schema))
                {
                    Schema = RecordSchema.Parse(schema);
                }
            }
        }

        public byte[] Serialize(GenericRecord payload)
        {
            var buffer = new ByteBufferOutputStream();
            var encoder = new BinaryEncoder(buffer);
            var writer = new DefaultWriter(Schema);
            writer.Write<GenericRecord>(payload, encoder);
            buffer.Flush();
            var streams = buffer.GetBufferList();
            return streams[0].ToArray();
        }

        public byte[] Serialize<T>(T payload) where T : ISpecificRecord
        {
            using (var ms = new MemoryStream())
            {
                var encoder = new BinaryEncoder(ms);
                var writer = new SpecificDefaultWriter(Schema);
                writer.Write(Schema, payload, encoder);
                return ms.ToArray();
            }
        }

        public T Deserialize<T>(byte[] payload) where T: ISpecificRecord, new()
        {
            using (var ms = new MemoryStream(payload))
            {
                var decoder = new BinaryDecoder(ms);
                var newObj = new T();
                var reader = new SpecificDefaultReader(Schema, Schema);
                reader.Read(newObj, Schema, Schema, decoder);
                return newObj;
            }
        }
    }
}
