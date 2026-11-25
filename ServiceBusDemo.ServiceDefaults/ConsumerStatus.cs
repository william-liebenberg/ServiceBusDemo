using System.Text.Json.Serialization;

namespace ServiceBusDemo.ServiceDefaults;

[JsonConverter(typeof(JsonStringEnumConverter<ConsumerState>))]
public enum ConsumerState
{
    Unavailable,
    Initializing,
    Transitioning,
    Running,
    Paused,
    Mixed
}

public sealed record SubscriptionStatus(string Pattern, string Topic, string Subscription, ConsumerState State);

public sealed record ConsumerStatus(string Consumer, ConsumerState State, IReadOnlyList<SubscriptionStatus> Subscriptions);

public static class ConsumerControlProtocol
{
    public const string HeaderName = "X-ServiceBusDemo-Control";
    public const string HeaderValue = "1";
}
