using System;
using Confluent.Kafka;

// https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/streaming-kafka-dotnet-client-quickstart.htm

namespace OssProducerWithKafkaApi
{
    class ProducingMessages
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Demo for using Kafka APIs seamlessly with OSS");

            var config = new ProducerConfig {
                            BootstrapServers = "<bootstrap_servers_endpoint>", //usually of the form cell-1.streaming.[region code].oci.oraclecloud.com:9092
                            SslCaLocation = @"<path\to\root\ca\certificate\*.pem>",
                            SecurityProtocol = SecurityProtocol.SaslSsl,
                            SaslMechanism = SaslMechanism.Plain,
                            SaslUsername = @"<OCI_tenancy_name>/<your_OCI_username>/<stream_pool_OCID>",
                            SaslPassword = "<your_OCI_user_auth_token>", // use the auth-token you created step 5 of Prerequisites section 
                            };

            Produce("<topic_stream_name>", config); // use the name of the stream you created

        }

        static void Produce(string topic, ClientConfig config)
        {
            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                int numProduced = 0;
                int numMessages = 10;
                for (int i=0; i<numMessages; ++i)
                {
                    var key = "messageKey" + i;
                    var val = "messageVal" + i;

                    Console.WriteLine($"Producing record: {key} {val}");

                    producer.Produce(topic, new Message<string, string> { Key = key, Value = val },
                        (deliveryReport) =>
                        {
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                            }
                            else
                            {
                                Console.WriteLine($"Produced message to: {deliveryReport.TopicPartitionOffset}");
                                numProduced += 1;
                            }
                        });
                }

                producer.Flush(TimeSpan.FromSeconds(10));

                Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
            }
        }
    }
}