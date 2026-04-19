using System;
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

namespace RealTimeTrackerMonitor
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
    /// Background service that consumes messages from Kafka.
    /// This pattern uses IHostedService to run seamlessly in the background with DI and standard logging.
    /// </summary>
    public class ConsumerWorker : BackgroundService
    {
        private readonly ILogger _logger;
        // 1. Tracing
        // gives exact information about request details Act as detective 
        private static readonly ActivitySource ActivitySource = new ActivitySource("RealTimeTrackerMonitor");
        // 2. Metrics
        // meter and countre are both focus on aerage and total ,rates overtime giving helicopter view
        // How many message vehicle (x) was consumsd , how many messages where above average speed .
        // the factory
        // is just the namesapce tells the promethuse 'these numbers belong to the RealTimeTrackerMonitor application'
        private static readonly Meter Meter = new Meter("RealTimeTrackerMonitor.Metrics");
        
        //The tool (counter)
        private static readonly Counter<long> MessagesConsumedCounter = Meter.CreateCounter<long>("avl.messages.consumed.tracker");

        public ConsumerWorker()
        {
            _logger = Log.ForContext<ConsumerWorker>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.Information("Starting Real-Time Tracker Consumer Worker...");

            var server = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVER") ?? "localhost:9092";
            var topic = "avl-telemetry";

            var config = new ConsumerConfig
            {
                BootstrapServers = server,
                GroupId = "realtime-tracker-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        // Use a short timeout to allow checking stoppingToken regularly
                        var cr = consumer.Consume(TimeSpan.FromSeconds(1));
                        
                        if (cr == null) 
                            continue; // No message received within timeout

                        // [DISTRIBUTED TRACING] Extract the Trace ID from the Kafka message headers
                        string? traceParentId = null;
                        if (cr.Message.Headers != null && cr.Message.Headers.TryGetLastBytes("traceparent", out var bytes))
                        {
                            traceParentId = System.Text.Encoding.UTF8.GetString(bytes);
                        }

                        // Start span and link it to the producer's trace if we found one
                        using var activity = traceParentId != null 
                            ? ActivitySource.StartActivity("ProcessMessage", ActivityKind.Consumer, traceParentId)
                            : ActivitySource.StartActivity("ProcessMessage", ActivityKind.Consumer);

                        var record = JsonSerializer.Deserialize<AvlRecord>(cr.Message.Value);
                        if (record != null)
                        {
                            activity?.SetTag("vehicle.id", record.VehicleId);
                            activity?.SetTag("vehicle.speed", record.Speed);

                            _logger.Information("[TRACKER] {VehicleId} @ ({Latitude:F6}, {Longitude:F6}) - Current Speed: {Speed} km/h", 
                                record.VehicleId, record.Latitude, record.Longitude, record.Speed);
                                
                            MessagesConsumedCounter.Add(1, new System.Collections.Generic.KeyValuePair<string, object?>("vehicle.id", record.VehicleId));
                        }
                    }
                    catch (ConsumeException e)
                    {
                        _logger.Error(e, "Error occurred consuming message: {Reason}", e.Error.Reason);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.Information("Consumer worker is shutting down gracefully.");
            }
            finally
            {
                consumer.Close();
            }
            
            // To satisfy await requirement in async method yielding no tasks directly
            await Task.CompletedTask;
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            var seqUrl = Environment.GetEnvironmentVariable("SEQ_URL") ?? "http://localhost:5341";
            var otlpEndpoint = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT") ?? "http://localhost:4317";

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .Enrich.FromLogContext()
                .Enrich.WithProperty("ApplicationName", "RealTimeTrackerMonitor")
                .WriteTo.Console()
                .WriteTo.Seq(seqUrl)
                .CreateLogger();

            try
            {
                Log.Information("Configuring Host...");

                var builder = Host.CreateDefaultBuilder(args);
                builder.UseSerilog();

                builder.ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<ConsumerWorker>();

                    services.AddOpenTelemetry()
                        .WithTracing(tracer =>
                        {
                            tracer.AddSource("RealTimeTrackerMonitor")
                                  .SetResourceBuilder(OpenTelemetry.Resources.ResourceBuilder.CreateDefault().AddService("RealTimeTrackerMonitor"))
                                  .AddOtlpExporter(opt => opt.Endpoint = new Uri(otlpEndpoint));
                        })
                        .WithMetrics(metrics =>
                        {
                            metrics.AddMeter("RealTimeTrackerMonitor.Metrics")
                                   .AddRuntimeInstrumentation()
                                   .AddPrometheusHttpListener(opt => opt.UriPrefixes = new string[] { "http://*:9464/" });
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
