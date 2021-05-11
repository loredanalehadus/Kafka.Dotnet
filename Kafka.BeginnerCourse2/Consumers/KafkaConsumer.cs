using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Elasticsearch.Net;
using Kafka.BeginnerCourse2.Clients;
using Kafka.BeginnerCourse2.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Kafka.BeginnerCourse2.Consumers
{
    public class KafkaConsumer : IKafkaConsumer
    {
        private readonly ILogger logger;
        private readonly IElasticSearchClient elasticClient;
        private readonly ConsumerConfig config;
        private readonly KafkaOptions kafkaOptions;
        private readonly SchemaRegistryConfigOptions configOptions;

        public KafkaConsumer(ILogger<KafkaConsumer> logger,
            IOptions<KafkaOptions> kafkaOptions,
            IOptions<ProducerOptions> producerOptions,
            IOptions<SchemaRegistryConfigOptions> schemaRegistryOptions,
            IElasticSearchClient elasticClient)
        {
            this.logger = logger;
            this.elasticClient = elasticClient;
            this.kafkaOptions = kafkaOptions.Value;
            configOptions = schemaRegistryOptions.Value;

            config = new ConsumerConfig
            {
                BootstrapServers = kafkaOptions.Value.BootstrapServers,
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = configOptions.Username,
                SaslPassword = configOptions.Password,
                SslCaLocation = producerOptions.Value.SslCaLocation,
                GroupId = "kafka-demo-elastic-search",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                //EnableAutoCommit = false //disable autocommit and work in sync batches
            };
        }

        public async Task Consume()
        {
            var client = elasticClient.Create();

            try
            {
                var schemaRegistryConfig = new SchemaRegistryConfig
                {
                    Url = configOptions.Url,
                    BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
                    BasicAuthUserInfo = $"{configOptions.Username}:{configOptions.Password}",
                };

                using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
                using var consumer = new ConsumerBuilder<string, string>(config)
                    .SetStatisticsHandler((_, json) => Console.WriteLine($"Stats: {json}"))
                    .Build();

                consumer.Subscribe(kafkaOptions.Topic);

                while (true)
                {
                    //now the consumer will only read the messages from partition 0
                    var consumeResult = consumer.Consume(CancellationToken.None);

                    var indexResponse = await client.IndexAsync<Message>("messages", consumeResult.Message.Key, JsonConvert.SerializeObject(consumeResult.Message));

                    logger.LogInformation($"Elastic searchId: {indexResponse.Key}");
                    // handle consumed message.
                    logger.LogInformation("\nConsumed data: \n" +
                                          "Key: " + consumeResult.Message.Key + ", Value: " +
                                          consumeResult.Message.Value + "\n" +
                                          "Partition: " + consumeResult.Partition + "\n" +
                                          "Offset: " + consumeResult.Offset + "\n");

                }

                consumer.Commit();

            }
            catch (Exception e)
            {
                logger.LogError($"Error while consuming: {e.Message}");

            }

        }
    }

    public class Message : ElasticsearchResponseBase
    {
        public string Key { get; set; }
        public string Value { get; set; }
    }
}