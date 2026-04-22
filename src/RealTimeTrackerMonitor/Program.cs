using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Serilog;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
namespace RealTimeTrackerMonitor
{


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

                var builder = WebApplication.CreateBuilder(args);
                builder.Host.UseSerilog();

                // 1. Add your Background Worker
                builder.Services.AddHostedService<Background.ConsumerWorker>();


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


                builder.Services.AddOpenTelemetry()
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

                var app = builder.Build();

                // 4. Expose the /health URL
                app.MapHealthChecks("/health", new  HealthCheckOptions
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
