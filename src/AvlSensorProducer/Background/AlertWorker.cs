using AvlSensorProducer.Models;
using Confluent.Kafka;
using Serilog;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;

namespace AvlSensorProducer.Background
{

    /// <summary>
    /// Background service that acts as a Kafka Producer.
    /// By using IHostedService, we follow .NET best practices for background tasks,
    /// allowing the framework to cleanly manage logging, DI, and application lifecycle.
    /// </summary>
    public class ProducerWorker : BackgroundService
    {
        private readonly Serilog.ILogger _logger;
        // 1. Define ActivitySource for Tracing (Spans)
        private static readonly ActivitySource ActivitySource = new ActivitySource("AvlSensorProducer");
        // 2. Define Meter for Metrics
        private static readonly Meter Meter = new Meter("AvlSensorProducer.Metrics");
        // 3. Create a Counter to track the number of produced messages
        private static readonly Counter<long> MessagesProducedCounter = Meter.CreateCounter<long>("avl.messages.produced");

        public ProducerWorker()
        {
            _logger = Log.ForContext<ProducerWorker>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.Information("Starting AVL Sensor Producer Worker...");

            // Amman, Jordan center coordinates
            double ammanLat = 31.9539;
            double ammanLon = 35.9106;

            var random = new Random();
            var vehicles = new List<AvlRecord>();

            // Initialize 10 vehicles
            for (int i = 1; i <= 10; i++)
            {
                vehicles.Add(new AvlRecord
                {
                    VehicleId = $"JOR-VHC-{i:D3}",
                    Latitude = ammanLat + (random.NextDouble() - 0.5) * 0.1,
                    Longitude = ammanLon + (random.NextDouble() - 0.5) * 0.1,
                    Speed = random.Next(40, 90),
                    Timestamp = DateTimeOffset.UtcNow
                });
            }

            var server = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVER") ?? "localhost:9092";
            var topic = "avl-telemetry";

            var config = new ProducerConfig
            {
                BootstrapServers = server,
                ClientId = "SimulatorProducer"
            };

            _logger.Information("Connecting to Kafka at {Server}", server);

            using var producer = new ProducerBuilder<string, string>(config).Build();

            while (!stoppingToken.IsCancellationRequested)
            {
                foreach (var v in vehicles)
                {
                    v.Latitude += (random.NextDouble() - 0.5) * 0.001;
                    v.Longitude += (random.NextDouble() - 0.5) * 0.001;

                    v.Speed += random.Next(-10, 15);
                    if (v.Speed < 0) v.Speed = 0;
                    if (v.Speed > 130) v.Speed = 130;

                    v.Timestamp = DateTimeOffset.UtcNow;
                    var json = JsonSerializer.Serialize(v);

                    // Create an OpenTelemetry Trace Span for the produce operation
                    using var activity = ActivitySource.StartActivity("ProduceMessage", ActivityKind.Producer);
                    activity?.SetTag("vehicle.id", v.VehicleId);
                    activity?.SetTag("vehicle.speed", v.Speed);

                    try
                    {
                        var message = new Message<string, string>
                        {
                            Key = v.VehicleId,
                            Value = json
                        };

                        // [DISTRIBUTED TRACING] Inject the Trace ID into the Kafka message headers
                        if (activity != null)
                        {
                            message.Headers = new Headers();
                            message.Headers.Add("traceparent", System.Text.Encoding.UTF8.GetBytes(activity.Id ?? string.Empty));
                        }

                        var dr = await producer.ProduceAsync(topic, message);

                        // Structured Logging: Use semantic properties like {VehicleId} instead of string concatenation
                        _logger.Information("Produced record for {VehicleId} to {PartitionOffset}. Speed: {Speed} km/h",
                            v.VehicleId, dr.TopicPartitionOffset, v.Speed);

                        // Increment the Prometheus Counter
                        MessagesProducedCounter.Add(1, new KeyValuePair<string, object?>("vehicle.id", v.VehicleId));
                    }
                    catch (ProduceException<string, string> e)
                    {
                        _logger.Error(e, "Delivery failed: {Reason}", e.Error.Reason);
                        activity?.SetStatus(ActivityStatusCode.Error, e.Error.Reason);
                    }
                }

                await Task.Delay(2000, stoppingToken);
            }
        }
    }
}
