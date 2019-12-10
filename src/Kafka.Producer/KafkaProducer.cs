using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Kafka.Producer
{
    public class KafkaProducer 
    {

        private readonly IProducer<Null,string> _producer;

        public KafkaProducer()
        {
            var config = new ProducerConfig { BootstrapServers =  "10.104.51.12:9092,10.104.51.13:9092,10.104.51.14:9092"};
            
            _producer = new ProducerBuilder<Null,string>(config).Build();
            
            Console.WriteLine("\n-----------------------------------------------------------------------");
            Console.WriteLine("Ctrl-C to quit.\n");
        }


        public async Task Send(string message, string topic)
        {
            if (message == null) { throw new ArgumentNullException(nameof(message));}
            if (string.IsNullOrEmpty(topic)) { throw new ArgumentNullException(nameof(topic)); }

            try
            {
                // Note: Awaiting the asynchronous produce request below prevents flow of execution
                // from proceeding until the acknowledgement from the broker is received (at the 
                // expense of low throughput).
                var deliveryReport = await _producer.ProduceAsync(
                    topic,
                    new Message<Null, string> {Value = message});

                Console.WriteLine($"delivered to: {deliveryReport.TopicPartitionOffset}");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
            }
        }
    }
}