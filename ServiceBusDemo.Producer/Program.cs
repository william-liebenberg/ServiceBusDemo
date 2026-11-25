using Azure.Messaging.ServiceBus;

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
    PublishAsync(request.Message, sender, logger, cancellationToken))
.WithName("SendMessage");

// Publish/subscribe: published to a topic where each consumer has its own subscription.
app.MapPost("/broadcastMessage", (SendMessageRequest request, [FromKeyedServices(SenderKeys.BroadcastEvents)] ServiceBusSender sender, ILogger<Program> logger, CancellationToken cancellationToken) =>
    PublishAsync(request.Message, sender, logger, cancellationToken))
.WithName("BroadcastMessage");

// Topic filters: the broker routes independently on each subscription's rule.
app.MapPost("/routedMessage", (RoutedMessageRequest request, [FromKeyedServices(SenderKeys.RoutedEvents)] ServiceBusSender sender, ILogger<Program> logger, CancellationToken cancellationToken) =>
    PublishAsync(request.Message, sender, logger, cancellationToken, request.Priority, request.Region))
.WithName("RoutedMessage");

app.MapDefaultEndpoints();

app.Run();

static async Task<IResult> PublishAsync(string body, ServiceBusSender sender, ILogger logger, CancellationToken cancellationToken,
    string? priority = null, string? region = null)
{
    if (string.IsNullOrWhiteSpace(body))
    {
        return Results.BadRequest("Message is required.");
    }

    var message = new ServiceBusMessage(body)
    {
        MessageId = Guid.NewGuid().ToString(),
        ContentType = "text/plain"
    };
    if (!string.IsNullOrEmpty(priority))
    {
        message.ApplicationProperties["Priority"] = priority;
    }
    if (!string.IsNullOrEmpty(region))
    {
        message.ApplicationProperties["Region"] = region;
    }

    await sender.SendMessageAsync(message, cancellationToken);
    logger.LogInformation("Sent message {MessageId} to topic {Topic}: {Body}", message.MessageId, sender.EntityPath, body);

    return Results.Accepted(value: new { message.MessageId, Topic = sender.EntityPath });
}

record SendMessageRequest(string Message);
record RoutedMessageRequest(string Message, string? Priority = null, string? Region = null);

static class SenderKeys
{
    public const string BusinessEvents = "business-events";
    public const string BroadcastEvents = "broadcast-events";
    public const string RoutedEvents = "routed-events";
}
