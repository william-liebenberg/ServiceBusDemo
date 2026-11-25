using System.Text.Json.Serialization;

namespace ServiceBusDemo.Web;

public class ProducerApiClient(HttpClient httpClient)
{
    public async Task<SendMessageResult> SendMessageAsync(string message, MessagingPattern pattern,
        CancellationToken cancellationToken = default, string? priority = null, string? region = null)
    {
        var endpoint = pattern switch
        {
            MessagingPattern.CompetingConsumers => "/sendMessage",
            MessagingPattern.PubSub => "/broadcastMessage",
            MessagingPattern.TopicFilters => "/routedMessage",
            _ => throw new ArgumentOutOfRangeException(nameof(pattern), pattern, null)
        };

        using var response = pattern == MessagingPattern.TopicFilters
            ? await httpClient.PostAsJsonAsync(endpoint, new RoutedMessageRequest(message,
                string.IsNullOrEmpty(priority) ? null : priority, string.IsNullOrEmpty(region) ? null : region), cancellationToken)
            : await httpClient.PostAsJsonAsync(endpoint, new SendMessageRequest(message), cancellationToken);

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

    private sealed record RoutedMessageRequest(string Message,
        [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? Priority,
        [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? Region);
}

public enum MessagingPattern
{
    CompetingConsumers,
    PubSub,
    TopicFilters
}

public record SendMessageRequest(string Message);

public record SendMessageResult(string MessageId, string Topic);
