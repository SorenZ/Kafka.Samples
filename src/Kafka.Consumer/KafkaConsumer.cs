using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Kafka.Consumer
{
    public class KafkaConsumer : HostedService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<KafkaConsumer> _logger;

        public KafkaConsumer(IServiceProvider serviceProvider, ILogger<KafkaConsumer> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
        }


        protected override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var config = new Dictionary<string, object>
            {
                {"group.id", "mobin.notification-group"},
                {"bootstrap.servers", "10.104.51.12:9092,10.104.51.13:9092,10.104.51.14:9092"}
            };

            var consumer = new Consumer<Ignore, string>(config, new IgnoreDeserializer(), new StringDeserializer(Encoding.UTF8));

            consumer.OnConsumeError += OnConsumeError;
            consumer.OnError += OnError;
            consumer.Subscribe("mobin-soft");

            Task.Run(async () => await Consume(consumer, cancellationToken), cancellationToken);

            return Task.CompletedTask;
        }

        private void OnError(object sender, Error e)
        {
            _logger.LogError($" error consuming message <{e.Reason}> with reason <{e.Reason}", e);
        }

        private void OnConsumeError(object sender, Message e)
        {
            _logger.LogError($" error on-consume consuming message on topic <{e.Topic}> with reason <{e.Error}", e.Value);
        }

        private async Task Consume(Consumer<Ignore, string> consumer, CancellationToken cancellationToken)
        {
            string message = null;

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    if (!consumer.Consume(out var kafkaMessage, TimeSpan.FromSeconds(1)))
                    {
                        continue;
                    }
                    //_logger.LogInformation($" received message on topic <{kafkaMessage.Topic}>", kafkaMessage.Value);
                    // 1: The received message is deserialized to an IntegrationEvent which contains the meta-data for handling the event.
                    message = JsonConvert.DeserializeObject<string>(kafkaMessage.Value);

                    _logger.LogInformation($"{nameof(KafkaConsumer)}: received <{message}> on topic <{kafkaMessage.Topic}>");

                    // 5: The message is committed, if no exception occurred during handling of the message
                    await consumer.CommitAsync(kafkaMessage);
                }
                catch (Exception ex)
                {
                    if (message != null)
                    {
                        ex.Data.Add("IntegrationMessage", message);
                    }

                    _logger.LogError(ex,"error happened in Consume()");
                }
            }
        }
    }
}
