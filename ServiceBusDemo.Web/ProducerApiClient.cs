using System.Text.Json.Serialization;

namespace ServiceBusDemo.Web;

public class ProducerApiClient(HttpClient httpClient)
{
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

        if (!response.IsSuccessStatusCode)
        {
            var error = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException(
                $"Producer API returned {(int)response.StatusCode} ({response.ReasonPhrase}): {error}",
                inner: null,
                statusCode: response.StatusCode);
        }

        return await response.Content.ReadFromJsonAsync<SendMessageResult>(cancellationToken)
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
