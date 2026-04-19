using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Serilog;

namespace AvlSensorProducer
{
    public class AvlRecord
    {
        public string VehicleId { get; set; } = string.Empty;
        public double Latitude { get; set; }
        public double Longitude { get; set; }
        public double Speed { get; set; }
        public DateTimeOffset Timestamp { get; set; }
    }

    /// <summary>
    /// Background service that acts as a Kafka Producer.
    /// By using IHostedService, we follow .NET best practices for background tasks,
    /// allowing the framework to cleanly manage logging, DI, and application lifecycle.
    /// </summary>
    public class ProducerWorker : BackgroundService
    {
        private readonly ILogger _logger;
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

    class Program
    {
        static async Task Main(string[] args)
        {
            var seqUrl = Environment.GetEnvironmentVariable("SEQ_URL") ?? "http://localhost:5341";
            var otlpEndpoint = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT") ?? "http://localhost:4317";

            // Initialize Serilog first so we can catch startup errors
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .Enrich.FromLogContext()
                .Enrich.WithProperty("ApplicationName", "AvlSensorProducer")
                .WriteTo.Console()
                .WriteTo.Seq(seqUrl)
                .CreateLogger();

            try
            {
                Log.Information("Configuring Host...");

                var builder = Host.CreateDefaultBuilder(args);

                // Use Serilog for standard ILogger injection
                builder.UseSerilog();

                builder.ConfigureServices((hostContext, services) =>
                {
                    // Add background worker
                    services.AddHostedService<ProducerWorker>();

                    // Configure OpenTelemetry for Tracing and Metrics
                    services.AddOpenTelemetry()
                        .WithTracing(tracerProviderBuilder =>
                        {
                            tracerProviderBuilder
                                // 1. WHICH factory are we listening to?
                                .AddSource("AvlSensorProducer")
                                // 2. WHO is sending this data?
                                .SetResourceBuilder(OpenTelemetry.Resources.ResourceBuilder.CreateDefault().AddService("AvlSensorProducer"))
                                // 3. WHERE are we shipping it?
                                .AddOtlpExporter(opt =>
                                {
                                    opt.Endpoint = new Uri(otlpEndpoint);
                                });
                        })
                        .WithMetrics(metricsProviderBuilder =>
                        {
                            metricsProviderBuilder
                                // 1. WHICH metrics are we tracking?
                                .AddMeter("AvlSensorProducer.Metrics")
                                // 2. FREE extra metrics!
                                .AddRuntimeInstrumentation()
                                // 3. HOW do we share this data?
                                // Expose /metrics endpoint on port 9464 for Prometheus to scrape
                                .AddPrometheusHttpListener(options => options.UriPrefixes = new string[] { "http://*:9464/" });
                        });
                });

                var host = builder.Build();
                await host.RunAsync();
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Host terminated unexpectedly");
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }
    }
}
