using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;

namespace Kafka.Producer
{
    public interface IProducer
    {
        Task Send(object @event, string topic);
    }

    public class KafkaProducer : IProducer
    {

        private readonly ISerializingProducer<Null, string> _producer;

        public KafkaProducer()
        {
            var config = new Dictionary<string, object>
            {
                {"bootstrap.servers", "10.104.51.12:9092,10.104.51.13:9092,10.104.51.14:9092"},

                {"message.send.max.retries", 3},
                {
                    "default.topic.config", new Dictionary<string, object>
                    {
                        {"message.timeout.ms", "00:00:15"}
                    }
                }
            };
            _producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));
        }


        public async Task Send(object @event, string topic)
        {
            if (@event == null) { throw new ArgumentNullException(nameof(@event));}
            if (string.IsNullOrEmpty(topic)) { throw new ArgumentNullException(nameof(topic)); }

            var message = JsonConvert.SerializeObject(@event);

            var result = await _producer.ProduceAsync(topic, null, message);

            if (result.Error.Code != ErrorCode.NoError)
            {
                var errorReason = result.Error.Reason;
                throw new ArgumentException(errorReason);
            }

            Console.WriteLine($"{nameof(KafkaProducer)}: message sent on topic <{topic}>: ", @event);
        }
    }
}
