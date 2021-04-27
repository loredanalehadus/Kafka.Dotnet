using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka.BeginnerCourse
{
    class Program
    {
        private static List<(string key, string value)> Messages => new List<(string key, string value)>
        {
            ("key_1", "Hi there"),
            ("key_1", "My name is Loredana"),
            ("key_1", "kafka is interesting so far"),
            ("key_1", "just another message :)"),
            ("key_1", "I like it!")
        };

        static async Task Main(string[] args)
        {
            using var host = CreateHostBuilder(args).Build();
            using var serviceScope = host.Services.CreateScope();
            var provider = serviceScope.ServiceProvider;

            //create a kafka producer
            var producer = provider.GetRequiredService<ILocalProducer>();

            //produce messages to Kafka topic
            await producer.Produce(Messages);


            //create kafka consumer
            //using (var consumer = new ConsumerBuilder<Ignore, string>(config)
            //    //.SetValueDeserializer(customStringSerializerImpl)
            //    .SetStatisticsHandler((_, json) => Console.WriteLine($"Stats: {json}"))
            //    .SetPartitionsAssignedHandler((c, partitions) =>
            //    {
            //        // called on consumer group rebalance immediately before 
            //        // the consumer starts reading from the partitions. You
            //        // can override the start offsets, and even the set of
            //        // partitions to consume from by (optionally) returning
            //        // a list from this method.
            //    })
            //    .Build())
            //{
            //    //...
            //}

            //Console.WriteLine("Hello World!");
        }

        private static IHostBuilder
            CreateHostBuilder(string[] args)
        {
            return Host.CreateDefaultBuilder(args)
                .ConfigureLogging(logging =>
                {
                    logging.ClearProviders();
                    logging.AddConsole();
                })
                .ConfigureServices((_,
                        services) =>
                    services
                        .AddSingleton<ILocalProducer, LocalProducer>());
        }
    }
}
