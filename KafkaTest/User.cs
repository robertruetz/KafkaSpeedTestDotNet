using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Avro;

namespace KafkaTest
{
    class User : Avro.Specific.ISpecificRecord
    {
        public string Name;
        public int FavoriteNumber;
        public string FavoriteColor;
        public static Schema _SCHEMA = Avro.Schema.Parse(@"{ ""namespace"": ""example.avro"", ""type"": ""record"", ""name"": ""User"", ""fields"": [  {""name"": ""Name"", ""type"": ""string""},  {""name"": ""FavoriteNumber"", ""type"": [""int"", ""null""]},  {""name"": ""FavoriteColor"", ""type"": [""string"", ""null""]} ] }");

        public User()
        {
            
        }

        public User(string name, int favoriteNumber, string favoriteColor)
        {
            Name = name;
            FavoriteNumber = favoriteNumber;
            FavoriteColor = favoriteColor;
        }

        public virtual Schema Schema
        {
            get
            {
                return User._SCHEMA;
            }
        }

        public object Get(int fieldPos)
        {
            switch(fieldPos)
            {
                case 0: return this.Name;
                case 1: return this.FavoriteNumber;
                case 2: return this.FavoriteColor;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            }
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch(fieldPos)
            {
                case 0: this.Name = (System.String)fieldValue; break;
                case 1: this.FavoriteNumber = (int)fieldValue; break;
                case 2: this.FavoriteColor = (System.String)fieldValue; break;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put().");
            }
        }
    }


}
