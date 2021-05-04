using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Kafka.BeginnerCourse2.Configuration;
using Kafka.BeginnerCourse2.Consumers;
using Kafka.BeginnerCourse2.Producers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka.BeginnerCourse2
{
    public class Program
    {
        private static List<(string key, string value)> Messages => new List<(string key, string value)>
        {
            ("key_1", "Hi there"),
            ("key_2", "My name is Loredana"),
            ("key_3", "kafka is interesting so far"),
            ("key_4", "just another message :)"),
            ("key_5", "I like it!")
        };

        public static async Task Main(string[] args)
        {
            using var host = CreateHostBuilder(args).Build();
            using var serviceScope = host.Services.CreateScope();
            var provider = serviceScope.ServiceProvider;

            //create a kafka producer
            var producer = provider.GetRequiredService<IIdempotentProducer>();
            await producer.Produce(Messages);

            //create kafka consumer
            //var consumer = provider.GetRequiredService<IConsumer>();
            //consumer.Consume();

            Console.WriteLine("Hello World!");
        }

        private static IHostBuilder
            CreateHostBuilder(string[] args)
        {
            return Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((hostingContext, configuration) =>
                {
                    configuration.Sources.Clear();
                    configuration.AddJsonFile("appsettings.json", false, true);
                }).ConfigureLogging(logging =>
                {
                    logging.ClearProviders();
                    logging.AddConsole();
                })
                .ConfigureServices((context, services) =>
                {
                    services.Configure<ProducerOptions>(context.Configuration.GetSection(nameof(ProducerOptions)));
                    services.Configure<SchemaRegistryConfigOptions>(
                        context.Configuration.GetSection(nameof(SchemaRegistryConfigOptions)));

                    services
                        .AddSingleton<IProducer, Producer>()
                        .AddSingleton<IIdempotentProducer, IdempotentProducer>()
                        /*.AddSingleton<IConsumer, Consumer>()*/;
                });
        }
    }
}