using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Kafka.BeginnerCourse
{
    public class LocalConsumer : ILocalConsumer
    {
        private readonly ILogger logger;
        private readonly ConsumerConfig config;
        private readonly string topic = "first_topic";

        public LocalConsumer(ILogger<LocalConsumer> logger)
        {
            this.logger = logger;

            config = new ConsumerConfig
            {
                BootstrapServers = "127.0.0.1:9092",
                GroupId = "my_first_application",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        public async Task Consume(bool cancelled)
        {
            try
            {
                using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                    //.SetValueDeserializer(customStringSerializerImpl)
                    .SetStatisticsHandler((_, json) => Console.WriteLine($"Stats: {json}"))
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        // called on consumer group rebalance immediately before 
                        // the consumer starts reading from the partitions. You
                        // can override the start offsets, and even the set of
                        // partitions to consume from by (optionally) returning
                        // a list from this method.
                    })
                    .Build())
                {
                    consumer.Subscribe(topic);

                    while (!cancelled)
                    {
                        var consumeResult = consumer.Consume(CancellationToken.None);

                        // handle consumed message.
                        logger.LogInformation("\nConsumed data: \n" +
                                              "Key: " + consumeResult.Message.Key + ", Value: " + consumeResult.Message.Value + "\n" +
                                              "Partition: " + consumeResult.Partition + "\n" +
                                              "Offset: " + consumeResult.Offset + "\n");
                    }

                    consumer.Close();
                }
            }
            catch (Exception e)
            {
                logger.LogError($"Error while consuming: {e.Message}");
            }
        }
    }
}