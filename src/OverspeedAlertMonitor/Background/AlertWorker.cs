using Confluent.Kafka;
using OverspeedAlertMonitor.Models;
using Serilog;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;

namespace OverspeedAlertMonitor.Background
{

    /// <summary>
    /// Background service that consumes messages and fires alerts on overspeeding.
    /// Incorporates structured logging context for Seq and OpenTelemetry trace spans.
    /// </summary>
    public class AlertWorker : BackgroundService
    {
        private readonly Serilog.ILogger _logger;
        private static readonly ActivitySource ActivitySource = new ActivitySource("OverspeedAlertMonitor");
        private static readonly Meter Meter = new Meter("OverspeedAlertMonitor.Metrics");

        // Expose a gauge for alerts to Prometheus
        private static readonly Counter<long> OverspeedAlertsCounter = Meter.CreateCounter<long>("avl.alerts.overspeed");

        public AlertWorker()
        {
            _logger = Log.ForContext<AlertWorker>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.Information("Starting Overspeed Alert Consumer Worker...");

            var server = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVER") ?? "localhost:9092";
            var topic = "avl-telemetry";
            var speedThreshold = 100.0; // km/h

            var config = new ConsumerConfig
            {
                BootstrapServers = server,
                GroupId = "overspeed-alert-group",
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
                        var cr = consumer.Consume(TimeSpan.FromSeconds(1));

                        if (cr == null)
                            continue;

                        // [DISTRIBUTED TRACING] Extract the Trace ID from the Kafka message headers
                        string? traceParentId = null;
                        if (cr.Message.Headers != null && cr.Message.Headers.TryGetLastBytes("traceparent", out var bytes))
                        {
                            traceParentId = System.Text.Encoding.UTF8.GetString(bytes);
                        }

                        // Start span and link it to the producer's trace if we found one
                        using var activity = traceParentId != null
                            ? ActivitySource.StartActivity("EvaluateSpeed", ActivityKind.Consumer, traceParentId)
                            : ActivitySource.StartActivity("EvaluateSpeed", ActivityKind.Consumer);

                        var record = JsonSerializer.Deserialize<AvlRecord>(cr.Message.Value);
                        if (record != null)
                        {
                            activity?.SetTag("vehicle.id", record.VehicleId);
                            activity?.SetTag("vehicle.speed", record.Speed);

                            if (record.Speed > speedThreshold)
                            {
                                // Log specifically as a Warning for alerts so they stand out in Seq
                                _logger.Warning("[🚨 ALERT] Overspeeding Detected! Vehicle {VehicleId} is travelling at {Speed} km/h! Lat: {Latitude}, Lon: {Longitude}",
                                    record.VehicleId, record.Speed, record.Latitude, record.Longitude);

                                OverspeedAlertsCounter.Add(1, new System.Collections.Generic.KeyValuePair<string, object?>("vehicle.id", record.VehicleId));
                            }
                            else
                            {
                                // Optionally log debug messages
                                _logger.Debug("Vehicle {VehicleId} is within speed limits.", record.VehicleId);
                            }
                        }
                    }
                    catch (ConsumeException e)
                    {
                        _logger.Error(e, "Error occurred evaluating record: {Reason}", e.Error.Reason);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.Information("Alert worker shutting down gracefully.");
            }
            finally
            {
                consumer.Close();
            }

            await Task.CompletedTask;
        }
    }
}
