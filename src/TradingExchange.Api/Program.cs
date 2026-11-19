using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using TradingExchange.Api.Hubs;
using TradingExchange.Api.Infrastructure;
using TradingExchange.Api.Observability;
using TradingExchange.Api.Risk;
using TradingExchange.Api.Services;

var builder = WebApplication.CreateBuilder(args);

var connStr = builder.Configuration.GetConnectionString("DefaultConnection")
    ?? "Server=localhost,1433;Database=TradingExchangeDb;User Id=sa;Password=Exchange_Strong_Pass123!;TrustServerCertificate=True;";

builder.Services.AddDbContext<ExchangeDbContext>(o => o.UseSqlServer(connStr));
builder.Services.AddSingleton<IExchangeEventPublisher, KafkaEventPublisher>();
builder.Services.AddSingleton<IDeadLetterPublisher, KafkaDeadLetterPublisher>();
builder.Services.AddSingleton<OrderBookStore>();
builder.Services.AddSingleton<RiskValidationService>();
builder.Services.AddSingleton<TradingCommandService>();
builder.Services.AddHostedService<KafkaProjectionWorker>();
builder.Services.AddSignalR();
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var otlpEndpoint = builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"];
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService(ExchangeTelemetry.ServiceName, serviceVersion: "1.0.0"))
    .WithTracing(t =>
    {
        t.AddSource(ExchangeTelemetry.ActivitySourceName)
         .AddAspNetCoreInstrumentation()
         .AddHttpClientInstrumentation();

        if (!string.IsNullOrWhiteSpace(otlpEndpoint))
            t.AddOtlpExporter();
    })
    .WithMetrics(m =>
    {
        m.AddMeter(ExchangeTelemetry.MeterName)
         .AddAspNetCoreInstrumentation()
         .AddPrometheusExporter();
    });

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<ExchangeDbContext>();
    for (int i = 1; i <= 15; i++) { try { await db.Database.EnsureCreatedAsync(); break; } catch when (i < 15) { await Task.Delay(1000); } }
}

app.UseSwagger();
app.UseSwaggerUI();
app.UseDefaultFiles();
app.UseStaticFiles();
app.MapPrometheusScrapingEndpoint();
app.MapControllers();
app.MapHub<MarketDataHub>("/hubs/marketdata");
app.MapGet("/health", () => Results.Ok(new { status = "healthy", service = "TradingExchange.Api" }));

app.Run();

public partial class Program;
