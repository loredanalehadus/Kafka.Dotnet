using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Kafka.BeginnerCourse
{
    public interface ILocalProducer
    {
        Task Produce(List<(string key, string value)> messages);
    }

    public class LocalProducer : ILocalProducer
    {
        private readonly ILogger logger;
        private readonly ProducerConfig config;
        private readonly string topic = "first_topic";

        public LocalProducer(ILogger<LocalProducer> logger)
        {
            this.logger = logger;

            config = new ProducerConfig
            {
                BootstrapServers = "127.0.0.1:9092",
                ClientId = Dns.GetHostName(),
            };
        }

        public async Task Produce(List<(string key, string value)> messages)
        {
            try
            {
                using var producer = new ProducerBuilder<string, string>(config).Build();

                foreach (var message in messages)
                {

                    var result = await producer.ProduceAsync(topic,
                        new Message<string, string> { Key = message.key, Value = message.value });

                    //by sending the key you guarantee that the same key always go to the same partition
                    logger.LogInformation($"Key: {message.key}");

                    logger.LogInformation("\nReceived new metadata. \n" +
                                          "Topic: " + result.Topic + "\n" +
                                          "Partition: " + result.Partition + "\n" +
                                          "Offset: " + result.Offset + "\n" +
                                          "Timestamp: " + result.Timestamp.UtcDateTime + "\n");
                }
            }
            catch (Exception e)
            {
                logger.LogError($"Error while producing: {e.Message}");
            }
        }
    }

    public interface ILocalConsumer
    {
        Task Consume();
    }
}