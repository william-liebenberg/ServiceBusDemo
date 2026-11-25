using System.Net;
using System.Net.Http.Json;
using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Web;

public abstract class ConsumerControlClient(HttpClient client, string consumer, int subscriptionCount)
{
    public async Task<ConsumerStatus> GetStatusAsync(CancellationToken token)
    {
        using var response = await client.GetAsync("/control/status", token);
        response.EnsureSuccessStatusCode();
        return await ReadStatusAsync(response, token);
    }

    public Task<ConsumerStatus> PauseAsync(CancellationToken token) => ChangeAsync("pause", ConsumerState.Paused, token);
    public Task<ConsumerStatus> ResumeAsync(CancellationToken token) => ChangeAsync("resume", ConsumerState.Running, token);

    private async Task<ConsumerStatus> ChangeAsync(string action, ConsumerState target, CancellationToken token)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"/control/{action}");
        request.Headers.Add(ConsumerControlProtocol.HeaderName, ConsumerControlProtocol.HeaderValue);
        using var response = await client.SendAsync(request, token);
        if (response.StatusCode == HttpStatusCode.ServiceUnavailable && response.Content.Headers.ContentType?.MediaType == "application/json")
        {
            throw new ConsumerTransitionException(await ReadStatusAsync(response, token));
        }
        response.EnsureSuccessStatusCode();
        var status = await ReadStatusAsync(response, token);
        return status.State == target ? status : throw new ConsumerTransitionException(status);
    }

    private async Task<ConsumerStatus> ReadStatusAsync(HttpResponseMessage response, CancellationToken token)
    {
        var status = await response.Content.ReadFromJsonAsync<ConsumerStatus>(token);
        if (status?.Consumer != consumer || !Enum.IsDefined(status.State)
            || status.Subscriptions is null || status.Subscriptions.Count != subscriptionCount
            || status.Subscriptions.Any(subscription => subscription is null || !Enum.IsDefined(subscription.State))
            || (status.State is ConsumerState.Running or ConsumerState.Paused
                && status.Subscriptions.Any(subscription => subscription.State != status.State)))
        {
            throw new InvalidOperationException("The consumer returned an invalid status.");
        }
        return status;
    }
}

public sealed class ConsumerTransitionException(ConsumerStatus status) : Exception("The consumer did not complete the requested transition.")
{
    public ConsumerStatus Status { get; } = status;
}

public sealed class ConsumerOneControlClient(HttpClient client)
    : ConsumerControlClient(client, BurstDeliveryTracker.ConsumerOneId, subscriptionCount: 4);

public sealed class ConsumerTwoControlClient(HttpClient client)
    : ConsumerControlClient(client, BurstDeliveryTracker.ConsumerTwoId, subscriptionCount: 3);

internal static class ConsumerControlClientRegistration
{
    public static IServiceCollection AddConsumerControlClient<T>(this IServiceCollection services, string address)
        where T : ConsumerControlClient
    {
        var builder = services.AddHttpClient<T>(client =>
            {
                client.BaseAddress = new(address);
                client.Timeout = TimeSpan.FromSeconds(15);
            })
            .ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler { AllowAutoRedirect = false });
        // Do not replay a state change after another tab has requested a conflicting action.
#pragma warning disable EXTEXP0001
        builder.RemoveAllResilienceHandlers();
#pragma warning restore EXTEXP0001
        return services;
    }
}
