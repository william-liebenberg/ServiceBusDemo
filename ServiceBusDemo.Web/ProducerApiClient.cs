namespace ServiceBusDemo.Web;

public class ProducerApiClient(HttpClient httpClient)
{
    public async Task<SendMessageResult> SendMessageAsync(string message, MessagingPattern pattern, CancellationToken cancellationToken = default)
    {
        var endpoint = pattern switch
        {
            MessagingPattern.CompetingConsumers => "/sendMessage",
            MessagingPattern.PubSub => "/broadcastMessage",
            _ => throw new ArgumentOutOfRangeException(nameof(pattern), pattern, null)
        };

        using var response = await httpClient.PostAsJsonAsync(endpoint, new SendMessageRequest(message), cancellationToken);

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
    PubSub
}

public record SendMessageRequest(string Message);

public record SendMessageResult(string MessageId, string Topic);
