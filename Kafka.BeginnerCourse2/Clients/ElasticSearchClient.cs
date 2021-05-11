using System;
using Elasticsearch.Net;
using Kafka.BeginnerCourse2.Configuration;
using Microsoft.Extensions.Options;

namespace Kafka.BeginnerCourse2.Clients
{
    public class ElasticSearchClient : IElasticSearchClient
    {
        private readonly ElasticClientOptions options;

        public ElasticSearchClient(IOptions<ElasticClientOptions> options)
        {
            this.options = options.Value;
        }

        public ElasticLowLevelClient Create()
        {
            var uri = new Uri(options.Url);
            var pool = new SingleNodeConnectionPool(uri);
            return new ElasticLowLevelClient(new ConnectionConfiguration(pool));
        }
    }
}