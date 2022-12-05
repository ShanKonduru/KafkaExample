using System;
using Confluent.Kafka;
using System.Threading;

namespace OssKafkaConsumerDotnet
{
    class ConsumingMessages
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Demo for using Kafka APIs seamlessly with OSS");

            var config = new ConsumerConfig
            {
                BootstrapServers = "<bootstrap_servers_endpoint>", //usually of the form cell-1.streaming.[region code].oci.oraclecloud.com:9092
                SslCaLocation = @"<path\to\root\ca\certificate\*.pem>",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = @"<OCI_tenancy_name>/<your_OCI_username>/<stream_pool_OCID>",
                SaslPassword = "<your_OCI_user_auth_token>", // use the auth-token you created step 5 of Prerequisites section 
            };

            Consume("<topic_stream_name>", config); // use the name of the stream you created
        }
        static void Consume(string topic, ClientConfig config)
        {
            var consumerConfig = new ConsumerConfig(config);
            consumerConfig.GroupId = "dotnet-oss-consumer-group";
            consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            consumerConfig.EnableAutoCommit = true;

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
            {
                consumer.Subscribe(topic);
                try
                {
                    while (true)
                    {
                        var cr = consumer.Consume(cts.Token);
                        string key = cr.Message.Key == null ? "Null" : cr.Message.Key;
                        Console.WriteLine($"Consumed record with key {key} and value {cr.Message.Value}");
                    }
                }
                catch (OperationCanceledException)
                {
                    //exception might have occurred since Ctrl-C was pressed.
                }
                finally
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    consumer.Close();
                }
            }
        }
    }
}