using System.Text.Json.Serialization;
using Azure.Messaging.ServiceBus;
using ServiceBusDemo.ServiceDefaults;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.AddAzureServiceBusClient(connectionName: "messaging");

var topicName = builder.Configuration["ServiceBus:TopicName"] ?? "business-events";
var broadcastTopicName = builder.Configuration["ServiceBus:BroadcastTopicName"] ?? "broadcast-events";
var routedTopicName = builder.Configuration["ServiceBus:RoutedTopicName"] ?? "routed-events";

builder.Services.AddKeyedSingleton(SenderKeys.BusinessEvents, (sp, _) => sp.GetRequiredService<ServiceBusClient>().CreateSender(topicName));
builder.Services.AddKeyedSingleton(SenderKeys.BroadcastEvents, (sp, _) => sp.GetRequiredService<ServiceBusClient>().CreateSender(broadcastTopicName));
builder.Services.AddKeyedSingleton(SenderKeys.RoutedEvents, (sp, _) => sp.GetRequiredService<ServiceBusClient>().CreateSender(routedTopicName));

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

// Competing consumers: published to a topic whose single subscription is shared by all consumers.
app.MapPost("/sendMessage", (SendMessageRequest request, [FromKeyedServices(SenderKeys.BusinessEvents)] ServiceBusSender sender, ILogger<Program> logger, CancellationToken cancellationToken) =>
    PublishAsync(request, sender, logger, cancellationToken))
.WithName("SendMessage");

// Publish/subscribe: published to a topic where each consumer has its own subscription.
app.MapPost("/broadcastMessage", (SendMessageRequest request, [FromKeyedServices(SenderKeys.BroadcastEvents)] ServiceBusSender sender, ILogger<Program> logger, CancellationToken cancellationToken) =>
    PublishAsync(request, sender, logger, cancellationToken))
.WithName("BroadcastMessage");

// Topic filters: the broker routes independently on each subscription's rule.
app.MapPost("/routedMessage", (SendMessageRequest request, [FromKeyedServices(SenderKeys.RoutedEvents)] ServiceBusSender sender, ILogger<Program> logger, CancellationToken cancellationToken) =>
    PublishAsync(request, sender, logger, cancellationToken, routed: true))
.WithName("RoutedMessage");

app.MapDefaultEndpoints();

app.Run();

static async Task<IResult> PublishAsync(SendMessageRequest request, ServiceBusSender sender, ILogger logger, CancellationToken cancellationToken,
    bool routed = false)
{
    if (string.IsNullOrWhiteSpace(request.Message))
    {
        return Results.BadRequest("Message is required.");
    }
    if (request.FailFirstCount is < FailureLab.Minimum or > FailureLab.MaxFailFirstCount)
    {
        return Results.BadRequest($"Fail-first count must be from {FailureLab.Minimum} to {FailureLab.MaxFailFirstCount}.");
    }
    if (request.DelaySeconds is < FailureLab.Minimum or > FailureLab.MaxDelaySeconds)
    {
        return Results.BadRequest($"Delay seconds must be from {FailureLab.Minimum} to {FailureLab.MaxDelaySeconds}.");
    }

    if (request.Subject is not null && !MessageMetadataRules.IsValid(request.Subject, MessageMetadataRules.NameLimit))
    {
        return Results.BadRequest($"Subject must be non-blank and at most {MessageMetadataRules.NameLimit} characters.");
    }

    if (request.CorrelationId is not null && !MessageMetadataRules.IsValid(request.CorrelationId, MessageMetadataRules.NameLimit))
    {
        return Results.BadRequest($"CorrelationId must be non-blank and at most {MessageMetadataRules.NameLimit} characters.");
    }

    if (request.ApplicationProperties is { } properties && properties.Any(property =>
            !MessageMetadataRules.IsValid(property.Key, MessageMetadataRules.NameLimit) ||
            !MessageMetadataRules.IsValid(property.Value, MessageMetadataRules.ValueLimit)))
    {
        return Results.BadRequest($"Application property keys and values must be non-blank and at most {MessageMetadataRules.NameLimit} and {MessageMetadataRules.ValueLimit} characters, respectively.");
    }

    var message = new ServiceBusMessage(request.Message)
    {
        MessageId = Guid.NewGuid().ToString(),
        ContentType = "text/plain"
    };
    if (request.Subject is not null)
    {
        message.Subject = request.Subject;
    }
    if (request.CorrelationId is not null)
    {
        message.CorrelationId = request.CorrelationId;
    }
    if (request.ApplicationProperties is not null)
    {
        foreach (var (key, value) in request.ApplicationProperties)
        {
            message.ApplicationProperties.Add(key, value);
        }
    }
    if (routed && !string.IsNullOrEmpty(request.Priority))
    {
        message.ApplicationProperties["Priority"] = request.Priority;
    }
    if (routed && !string.IsNullOrEmpty(request.Region))
    {
        message.ApplicationProperties["Region"] = request.Region;
    }
    if (request.FailFirstCount is int failFirstCount)
    {
        message.ApplicationProperties[FailureLab.FailFirstCountProperty] = failFirstCount;
    }
    if (request.AlwaysFail)
    {
        message.ApplicationProperties[FailureLab.AlwaysFailProperty] = true;
    }
    if (request.DelaySeconds is int delaySeconds)
    {
        message.ApplicationProperties[FailureLab.DelaySecondsProperty] = delaySeconds;
    }

    await sender.SendMessageAsync(message, cancellationToken);
    logger.LogInformation("Sent message {MessageId} to topic {Topic}: {Body}", message.MessageId, sender.EntityPath, request.Message);

    return Results.Accepted(value: new { message.MessageId, Topic = sender.EntityPath });
}

record SendMessageRequest(string Message,
    [property: JsonNumberHandling(JsonNumberHandling.Strict)] int? FailFirstCount = null,
    bool AlwaysFail = false,
    [property: JsonNumberHandling(JsonNumberHandling.Strict)] int? DelaySeconds = null,
    string? Subject = null, string? CorrelationId = null,
    Dictionary<string, string>? ApplicationProperties = null, string? Priority = null, string? Region = null);

public sealed class ProducerEndpointAssembly;

static class SenderKeys
{
    public const string BusinessEvents = "business-events";
    public const string BroadcastEvents = "broadcast-events";
    public const string RoutedEvents = "routed-events";
}

// Entry point for producer HTTP integration tests.
namespace ServiceBusDemo.Producer
{
    public sealed class ProducerApiMarker;
}
