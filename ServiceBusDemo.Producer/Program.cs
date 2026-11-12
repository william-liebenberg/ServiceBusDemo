using Azure.Messaging.ServiceBus;

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

    var message = new ServiceBusMessage(request.Message)
    {
        MessageId = Guid.NewGuid().ToString(),
        ContentType = "text/plain"
    };

    await sender.SendMessageAsync(message, cancellationToken);
    logger.LogInformation("Sent message {MessageId} to topic {Topic}: {Body}", message.MessageId, sender.EntityPath, request.Message);

    return Results.Accepted(value: new { message.MessageId, Topic = sender.EntityPath });
}

record SendMessageRequest(string Message);

static class SenderKeys
{
    public const string BusinessEvents = "business-events";
    public const string BroadcastEvents = "broadcast-events";
}
