using Elasticsearch.Net;

namespace Kafka.BeginnerCourse2.Clients
{
    public interface IElasticSearchClient
    {
        ElasticLowLevelClient Create();
    }
}