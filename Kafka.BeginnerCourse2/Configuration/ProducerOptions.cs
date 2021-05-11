using System.Net;
using Confluent.Kafka;

namespace Kafka.BeginnerCourse2.Configuration
{
    public class ProducerOptions
    {
        public string SslCaLocation { get; set; }

        public SecurityProtocol SecurityProtocol => SecurityProtocol.SaslSsl;
        public SaslMechanism SaslMechanism => SaslMechanism.Plain;
        public string ClientId => Dns.GetHostName();
        public bool EnableIdempotence => true;
        public Acks Acks => Acks.All;
        public int MessageSendMaxRetries => int.MaxValue;
        public int MaxInFlight => 5;

        //high throughput producer (at the expense of a bit of latency and CPU usage)
        public CompressionType CompressionType => CompressionType.Snappy;

        //waiting a bit for the messages to add up and send them all in bulk at once
        public int LingerMs => 20;

        public int BatchSize => 32 * 1024;
    }
}