using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafka.BeginnerCourse2.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kafka.BeginnerCourse2.Producers
{
    public interface IIdempotentProducer
    {
        Task Produce(List<(string key, string value)> messages);
    }

    public class IdempotentProducer : IIdempotentProducer
    {
        private readonly ILogger logger;
        private readonly ProducerConfig config;
        private readonly ProducerOptions producerOptions;
        private readonly SchemaRegistryConfigOptions configOptions;

        public IdempotentProducer(ILogger<IdempotentProducer> logger, IOptions<ProducerOptions> producerOptions, IOptions<SchemaRegistryConfigOptions> schemaRegistryOptions)
        {
            this.logger = logger;
            this.producerOptions = producerOptions.Value;
            configOptions = schemaRegistryOptions.Value;

            config = new ProducerConfig
            {
                BootstrapServers = producerOptions.Value.BootstrapServers,
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = producerOptions.Value.Username,
                SaslPassword = producerOptions.Value.Password,
                SslCaLocation = producerOptions.Value.SslCaLocation,
                ClientId = Dns.GetHostName(),
                EnableIdempotence = true,
                Acks = Acks.All,
                MessageSendMaxRetries = int.MaxValue,
                MaxInFlight = 5,
                //high throughput producer (at the expense of a bit of latency and CPU usage)
                CompressionType = CompressionType.Snappy,
                //waiting a bit for the messages to add up and send them all in bulk at once
                LingerMs = 20,
                BatchSize = 32 * 1024, //32 KB batch size
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
                    BasicAuthUserInfo = $"{configOptions.Username}:{configOptions.Password}",
                };

                using (CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(SchemaRegistryConfig))
                {
                    AvroSerializerConfig avroSerializerConfig = new AvroSerializerConfig { AutoRegisterSchemas = true };
                    //ISerializer<EventRaw> serializer =
                    //    new AvroSerializer<EventRaw>(schemaRegistry, avroSerializerConfig).AsSyncOverAsync();


                    using var producer = new ProducerBuilder<string, string>(config).Build();

                    foreach (var message in messages)
                    {
                        var result = await producer.ProduceAsync(producerOptions.Topic,
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