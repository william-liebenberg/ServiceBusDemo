using System.Net.Http.Json;
using Microsoft.Extensions.DependencyInjection;

namespace ServiceBusDemo.ServiceDefaults;

public sealed class DeliveryReporter(HttpClient client) : IDisposable
{
    public async Task<HttpResponseMessage> PostAsync(DeliveryEvent delivery, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/deliveries")
        {
            Content = JsonContent.Create(delivery)
        };
        return await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
    }

    public void Dispose() => client.Dispose();
}

public static class DeliveryReportingExtensions
{
    public static IHttpClientBuilder AddDeliveryReporting(this IServiceCollection services)
    {
        services.AddSingleton<DeliveryReportQueue>();
        services.AddHostedService<DeliveryReportWorker>();

        // Retain service discovery, but never retry or redirect a delivery POST.
#pragma warning disable EXTEXP0001 // Opt out of the resilience handlers added by AddServiceDefaults.
        return services.AddHttpClient<DeliveryReporter>(client =>
            {
                client.BaseAddress = new("https+http://webfrontend");
                client.Timeout = TimeSpan.FromSeconds(2);
            })
            .RemoveAllResilienceHandlers()
#pragma warning restore EXTEXP0001
            .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
            {
                AllowAutoRedirect = false,
                // The hosted worker holds the typed client for the lifetime of the process.
                PooledConnectionLifetime = TimeSpan.FromMinutes(2)
            });
    }
}
