using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Kafka.BeginnerCourse
{
    public class LocalConsumerAssignAndSeek : ILocalConsumerAssignAndSeek
    {
        private readonly ILogger logger;
        private readonly ConsumerConfig config;
        private readonly string topic = "first_topic";

        public LocalConsumerAssignAndSeek(ILogger<LocalConsumerAssignAndSeek> logger)
        {
            this.logger = logger;

            config = new ConsumerConfig
            {
                BootstrapServers = "127.0.0.1:9092",
                GroupId = "my_second_application",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        public async Task Consume()
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
                    //assign
                    var partitionToReadFrom = new TopicPartition(topic, 0);
                    var offsetToReadFrom = new TopicPartitionOffset(partitionToReadFrom, 15);
                    consumer.Assign(partitionToReadFrom);

                    //seek
                    consumer.Seek(offsetToReadFrom);

                    var numberOfMessagesToRead = 5;
                    var keepOnReading = true;
                    var numberOfMessagesReadSoFar = 0;

                    while (keepOnReading)
                    {
                        //now the consumer will only read the messages from partition 0
                        var consumeResult = consumer.Consume(CancellationToken.None);
                        numberOfMessagesReadSoFar++;

                        // handle consumed message.
                        logger.LogInformation("\nConsumed data: \n" +
                                              "Key: " + consumeResult.Message.Key + ", Value: " + consumeResult.Message.Value + "\n" +
                                              "Partition: " + consumeResult.Partition + "\n" +
                                              "Offset: " + consumeResult.Offset + "\n");
                        
                        if (numberOfMessagesReadSoFar < numberOfMessagesToRead) continue;
                        keepOnReading = false;
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
    public interface ILocalConsumerAssignAndSeek
    {
        Task Consume();
    }
}