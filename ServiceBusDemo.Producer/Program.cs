using Azure.Messaging.ServiceBus;
using ServiceBusDemo.ServiceDefaults;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.AddAzureServiceBusClient(connectionName: "messaging");

var topicName = builder.Configuration["ServiceBus:TopicName"] ?? "business-events";
var broadcastTopicName = builder.Configuration["ServiceBus:BroadcastTopicName"] ?? "broadcast-events";

builder.Services.AddKeyedSingleton(SenderKeys.BusinessEvents, (sp, _) => sp.GetRequiredService<ServiceBusClient>().CreateSender(topicName));
builder.Services.AddKeyedSingleton(SenderKeys.BroadcastEvents, (sp, _) => sp.GetRequiredService<ServiceBusClient>().CreateSender(broadcastTopicName));

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

app.MapDefaultEndpoints();

app.Run();

static async Task<IResult> PublishAsync(SendMessageRequest request, ServiceBusSender sender, ILogger logger, CancellationToken cancellationToken)
{
    if (string.IsNullOrWhiteSpace(request.Message))
    {
        return Results.BadRequest("Message is required.");
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

    await sender.SendMessageAsync(message, cancellationToken);
    logger.LogInformation("Sent message {MessageId} to topic {Topic}: {Body}", message.MessageId, sender.EntityPath, request.Message);

    return Results.Accepted(value: new { message.MessageId, Topic = sender.EntityPath });
}

record SendMessageRequest(string Message, string? Subject = null, string? CorrelationId = null,
    Dictionary<string, string>? ApplicationProperties = null);

public sealed class ProducerEndpointAssembly;

static class SenderKeys
{
    public const string BusinessEvents = "business-events";
    public const string BroadcastEvents = "broadcast-events";
}
