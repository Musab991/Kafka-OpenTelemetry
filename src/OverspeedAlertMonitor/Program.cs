using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OverspeedAlertMonitor.Background;
using Serilog;

namespace OverspeedAlertMonitor
{
    

    class Program
    {
        static async Task Main(string[] args)
        {
            var seqUrl = Environment.GetEnvironmentVariable("SEQ_URL") ?? "http://localhost:5341";
            var otlpEndpoint = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT") ?? "http://localhost:4317";

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug() // Allow debug logs to be emitted here
                .Enrich.FromLogContext()
                .Enrich.WithProperty("ApplicationName", "OverspeedAlertMonitor")
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

                builder.Services.AddHostedService<AlertWorker>();

                    builder.Services.AddOpenTelemetry()
                        .WithTracing(tracer =>
                        {
                            tracer.AddSource("OverspeedAlertMonitor")
                                  .SetResourceBuilder(OpenTelemetry.Resources.ResourceBuilder.CreateDefault().AddService("OverspeedAlertMonitor"))
                                  .AddOtlpExporter(opt => opt.Endpoint = new Uri(otlpEndpoint));
                        })
                        .WithMetrics(metrics =>
                        {
                            //which meter are we tracking?
                            metrics.AddMeter("OverspeedAlertMonitor.Metrics")
                            //trace cpu and memory extra free tracking       !
                            .AddRuntimeInstrumentation()
                            //Who do we share these data with ? 
                                   .AddPrometheusHttpListener(opt => opt.UriPrefixes = new string[] { "http://*:9464/" });
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
