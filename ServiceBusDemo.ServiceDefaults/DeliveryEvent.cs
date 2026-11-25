using System.Text.Json.Serialization;

namespace ServiceBusDemo.ServiceDefaults;

public sealed record DeliveryEvent(
    [property: JsonRequired] string MessageId,
    [property: JsonRequired] string Pattern,
    [property: JsonRequired] string Consumer,
    [property: JsonRequired] string Topic,
    [property: JsonRequired] string Subscription,
    [property: JsonRequired] string Body,
    [property: JsonRequired, JsonNumberHandling(JsonNumberHandling.Strict)] int DeliveryCount,
    [property: JsonRequired] DateTimeOffset ReceivedAt,
    [property: JsonRequired] string Outcome,
    [property: JsonRequired] DateTimeOffset InitialLockedUntil,
    [property: JsonRequired] DateTimeOffset SettledAt,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? Subject = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? CorrelationId = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] IReadOnlyDictionary<string, string>? ApplicationProperties = null);

public static class DeliveryOutcomes
{
    public const string Completed = nameof(Completed);
    public const string Abandoned = nameof(Abandoned);
    public const string DeadLettered = nameof(DeadLettered);
}
