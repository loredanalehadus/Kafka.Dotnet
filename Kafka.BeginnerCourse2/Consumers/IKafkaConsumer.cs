using System.Threading.Tasks;

namespace Kafka.BeginnerCourse2.Consumers
{
    public interface IKafkaConsumer
    {
        Task Consume();
    }
}