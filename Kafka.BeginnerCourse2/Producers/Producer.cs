using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Kafka.BeginnerCourse2.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kafka.BeginnerCourse2.Producers
{
    public interface IProducer
    {
        Task Produce(List<(string key, string value)> messages);
    }

    public class Producer : IProducer
    {
        private readonly ILogger logger;
        private readonly ProducerConfig config;
        private readonly KafkaOptions kafkaOptions;
        private readonly SchemaRegistryConfigOptions configOptions;

        public Producer(ILogger<Producer> logger,
                        IOptions<KafkaOptions> kafkaOptions,
                        IOptions<ProducerOptions> producerOptions,
                        IOptions<SchemaRegistryConfigOptions> schemaRegistryOptions)
        {
            this.logger = logger;
            this.kafkaOptions = kafkaOptions.Value;
            configOptions = schemaRegistryOptions.Value;

            config = new ProducerConfig
            {
                BootstrapServers = kafkaOptions.Value.BootstrapServers,
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = configOptions.Username,
                SaslPassword = configOptions.Password,
                SslCaLocation = producerOptions.Value.SslCaLocation,
                ClientId = Dns.GetHostName(),
            };
        }

        public async Task Produce(List<(string key, string value)> messages)
        {
            try
            {
                SchemaRegistryConfig SchemaRegistryConfig = new SchemaRegistryConfig
                {
                    Url = configOptions.Url,
                    BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
                    BasicAuthUserInfo = $"{configOptions.Username}:{configOptions.Password}"
                };

                using (CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(SchemaRegistryConfig))
                {
                    using var producer = new ProducerBuilder<string, string>(config).Build();

                    foreach (var message in messages)
                    {
                        var result = await producer.ProduceAsync(kafkaOptions.Topic,
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
            }
            catch (Exception e)
            {
                logger.LogError($"Error while producing: {e.Message}");
            }
        }
    }
}