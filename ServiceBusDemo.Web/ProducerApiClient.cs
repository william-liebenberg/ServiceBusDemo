using System.Globalization;
using System.Text.Json.Serialization;

namespace ServiceBusDemo.Web;

public class ProducerApiClient(HttpClient httpClient, IHttpClientFactory? clientFactory = null)
{
    internal const string DeadLetterActionsClient = "producer-dead-letter-actions";

    public Task<SendMessageResult> SendMessageAsync(string message, MessagingPattern pattern, CancellationToken cancellationToken = default,
        string? priority = null, string? region = null) =>
        SendMessageAsync(new SendMessageRequest(message,
            Priority: pattern == MessagingPattern.TopicFilters && !string.IsNullOrEmpty(priority) ? priority : null,
            Region: pattern == MessagingPattern.TopicFilters && !string.IsNullOrEmpty(region) ? region : null), pattern, cancellationToken);

    public async Task<SendMessageResult> SendMessageAsync(SendMessageRequest request, MessagingPattern pattern, CancellationToken cancellationToken = default)
    {
        var endpoint = pattern switch
        {
            MessagingPattern.CompetingConsumers => "/sendMessage",
            MessagingPattern.PubSub => "/broadcastMessage",
            MessagingPattern.TopicFilters => "/routedMessage",
            _ => throw new ArgumentOutOfRangeException(nameof(pattern), pattern, null)
        };

        using var response = await httpClient.PostAsJsonAsync(endpoint, request, cancellationToken);
        return await ReadAsync<SendMessageResult>(response, cancellationToken);
    }

    public async Task<DeadLetterSubscription[]> GetDeadLetterSubscriptionsAsync(CancellationToken token = default)
    {
        using var response = await httpClient.GetAsync("/dead-letters/subscriptions", token);
        return await ReadAsync<DeadLetterSubscription[]>(response, token);
    }

    public async Task<DeadLetterPage> PeekDeadLettersAsync(DeadLetterSubscription pair, long? fromSequenceNumber = null, CancellationToken token = default)
    {
        var cursor = fromSequenceNumber?.ToString(CultureInfo.InvariantCulture);
        var path = $"/dead-letters/messages?{PairQuery(pair)}" + (cursor is null ? "" : $"&fromSequenceNumber={cursor}");
        using var response = await httpClient.GetAsync(path, token);
        return await ReadAsync<DeadLetterPage>(response, token);
    }

    public Task<DeadLetterActionResult> ResubmitDeadLetterAsync(DeadLetterSubscription pair, long sequenceNumber, CancellationToken token = default) =>
        ActOnDeadLetterAsync(pair, sequenceNumber, "resubmit", token);

    public Task<DeadLetterActionResult> PurgeDeadLetterAsync(DeadLetterSubscription pair, long sequenceNumber, CancellationToken token = default) =>
        ActOnDeadLetterAsync(pair, sequenceNumber, "purge", token);

    private async Task<DeadLetterActionResult> ActOnDeadLetterAsync(DeadLetterSubscription pair, long sequenceNumber, string action, CancellationToken token)
    {
        using var actionClient = clientFactory?.CreateClient(DeadLetterActionsClient);
        var path = $"/dead-letters/messages/{sequenceNumber.ToString(CultureInfo.InvariantCulture)}/{action}?{PairQuery(pair)}";
        using var response = await (actionClient ?? httpClient).PostAsync(path, content: null, token);
        return await ReadAsync<DeadLetterActionResult>(response, token);
    }

    private static string PairQuery(DeadLetterSubscription pair) =>
        $"topic={Uri.EscapeDataString(pair.Topic)}&subscription={Uri.EscapeDataString(pair.Subscription)}";

    private static async Task<T> ReadAsync<T>(HttpResponseMessage response, CancellationToken token)
    {
        if (!response.IsSuccessStatusCode)
        {
            var error = await response.Content.ReadAsStringAsync(token);
            throw new HttpRequestException(
                $"Producer API returned {(int)response.StatusCode} ({response.ReasonPhrase}): {error}",
                inner: null,
                statusCode: response.StatusCode);
        }

        return await response.Content.ReadFromJsonAsync<T>(token)
            ?? throw new InvalidOperationException("Producer API returned an empty response.");
    }
}

public enum MessagingPattern
{
    CompetingConsumers,
    PubSub,
    TopicFilters
}

public record SendMessageRequest(string Message,
    int? FailFirstCount = null,
    bool AlwaysFail = false,
    int? DelaySeconds = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? Subject = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? CorrelationId = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] IReadOnlyDictionary<string, string>? ApplicationProperties = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? Priority = null,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? Region = null);

public record SendMessageResult(string MessageId, string Topic);

public sealed record DeadLetterSubscription(string Topic, string Subscription);
public sealed record DeadLetterEntry(long SequenceNumber, string MessageId, string? DeadLetterReason,
    string? DeadLetterErrorDescription, string Body, IReadOnlyDictionary<string, string> ApplicationProperties, int DeliveryCount);
public sealed record DeadLetterPage(DeadLetterEntry[] Messages, long? NextFromSequenceNumber);
public sealed record DeadLetterActionResult(string? PublishedMessageId);
