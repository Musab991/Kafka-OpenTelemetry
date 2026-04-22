using AvlSensorProducer.Background;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Serilog;

namespace AvlSensorProducer
{
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

                var builder = WebApplication.CreateBuilder(args);
                builder.Host.UseSerilog();


                #region KAFKA Health Check 
                var kafkaServer = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVER");
                // 1. Give the Security Guard the walkie-talkie
                builder.Services.AddHealthChecks()
                    .AddKafka(setup =>
                    {
                        setup.BootstrapServers = kafkaServer;
                        setup.MessageTimeoutMs = 1500; // Important: Don't wait forever! Fail fast if unreachable.
                    },
                    name: "kafka_broker",
                    tags: new[] { "messaging", "ready" }); // Tags help you filter checks later
                #endregion

                // Add background worker
                builder.Services.AddHostedService<ProducerWorker>();

                // Configure OpenTelemetry for Tracing and Metrics
                builder.Services.AddOpenTelemetry()
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



                var app = builder.Build();


                app.MapHealthChecks("/health", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
                {
                    ResponseWriter = async (context, report) =>
                    {
                        context.Response.ContentType = "application/json";
                        var response = new
                        {
                            Status = report.Status.ToString(),
                            Checks = report.Entries.Select(e => new
                            {
                                Component = e.Key,
                                Status = e.Value.Status.ToString(),
                                Error = e.Value.Exception?.Message ?? e.Value.Description
                            })
                        };
                        await JsonSerializer.SerializeAsync(context.Response.Body, response);
                    }
                });

                await app.RunAsync();
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
