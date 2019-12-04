using System;
using System.Threading.Tasks;

namespace Kafka.Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var producer = new KafkaProducer();

            while (true)
            {
                var message = Console.ReadLine();
                await producer.Send(message, "mobin-soft");
            }
            
        }


    }
}
