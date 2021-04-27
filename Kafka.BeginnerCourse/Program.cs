using System.Collections.Generic;
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
            ("key_2", "My name is Loredana"),
            ("key_3", "kafka is interesting so far"),
            ("key_4", "just another message :)"),
            ("key_5", "I like it!")
        };

        static async Task Main(string[] args)
        {
            using var host = CreateHostBuilder(args).Build();
            using var serviceScope = host.Services.CreateScope();
            var provider = serviceScope.ServiceProvider;

            //create a kafka producer
            var producer = provider.GetRequiredService<ILocalProducer>();
            await producer.Produce(Messages);

            //create kafka consumer
            var consumer = provider.GetRequiredService<ILocalConsumer>();
            await consumer.Consume(false);
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
                .ConfigureServices((_, services) =>
                    services
                        .AddSingleton<ILocalProducer, LocalProducer>()
                        .AddSingleton<ILocalConsumer, LocalConsumer>());
        }
    }
}
