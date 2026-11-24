using System.Net.Http.Json;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;

namespace ServiceBusDemo.Tests;

internal static class ControlTestClients
{
    public static void AddControlTestClients(this IServiceCollection services,
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? send = null)
    {
        services.AddHttpClient<ConsumerOneControlClient>(http => http.BaseAddress = new Uri("http://consumer-one.test"))
            .ConfigurePrimaryHttpMessageHandler(() => new Handler(send ?? WaitUntilDisposedAsync));
        services.AddHttpClient<ConsumerTwoControlClient>(http => http.BaseAddress = new Uri("http://consumer-two.test"))
            .ConfigurePrimaryHttpMessageHandler(() => new Handler(send ?? WaitUntilDisposedAsync));
    }

    // Tests unrelated to consumer control should not receive background status renders.
    private static async Task<HttpResponseMessage> WaitUntilDisposedAsync(HttpRequestMessage request, CancellationToken token)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, token);
        throw new InvalidOperationException("Expected page disposal to cancel status polling.");
    }

    public static ConsumerStatus Status(string consumer, ConsumerState state) => new(consumer, state,
        [new("CompetingConsumers", "business-events", "business-events", state == ConsumerState.Mixed ? ConsumerState.Running : state),
         new("PubSub", "broadcast-events", consumer == "ConsumerOne" ? "consumer-one" : "consumer-two",
             state == ConsumerState.Mixed ? ConsumerState.Paused : state)]);

    public static HttpResponseMessage Reply(string consumer, ConsumerState state, HttpStatusCode code = HttpStatusCode.OK) =>
        new(code) { Content = JsonContent.Create(Status(consumer, state)) };

    public sealed class Handler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            send(request, cancellationToken);
    }
}
