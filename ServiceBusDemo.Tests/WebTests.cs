using System.Collections.Concurrent;
using System.Net.Http.Json;
using Aspire.Hosting;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;

namespace ServiceBusDemo.Tests;

public class WebTests
{
    [Fact]
    public async Task ConsumersReportBothPatternsAndKeepProcessingWhenWebStops()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        var reports = new ConcurrentQueue<(DeliveryEvent Delivery, HttpStatusCode Status)>();
        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.ServiceBusDemo_AppHost>(token);

        // Observe reports by forwarding them to the real endpoint, without adding a read API.
        var proxyBuilder = WebApplication.CreateBuilder();
        proxyBuilder.WebHost.UseUrls("http://127.0.0.1:0");
        await using var proxy = proxyBuilder.Build();
        var webReady = new TaskCompletionSource<HttpClient>(TaskCreationOptions.RunContinuationsAsynchronously);
        proxy.MapPost("/api/deliveries", async (DeliveryEvent delivery, CancellationToken requestToken) =>
        {
            HttpStatusCode status;
            try
            {
                var client = await webReady.Task.WaitAsync(requestToken);
                using var response = await client.PostAsJsonAsync("/api/deliveries", delivery, requestToken);
                status = response.StatusCode;
            }
            catch (HttpRequestException)
            {
                status = HttpStatusCode.ServiceUnavailable;
            }
            catch (OperationCanceledException) when (requestToken.IsCancellationRequested)
            {
                status = HttpStatusCode.GatewayTimeout;
            }
            reports.Enqueue((delivery, status));
            return Results.StatusCode((int)status);
        });
        await proxy.StartAsync(token);
        foreach (var consumer in appHost.Resources.OfType<ProjectResource>().Where(resource => resource.Name.StartsWith("consumer-", StringComparison.Ordinal)))
        {
            consumer.Annotations.Add(new EnvironmentCallbackAnnotation(context =>
            {
                foreach (var key in context.EnvironmentVariables.Keys
                    .Where(key => key.StartsWith("services__webfrontend__", StringComparison.OrdinalIgnoreCase)).ToArray())
                {
                    context.EnvironmentVariables.Remove(key);
                }
                context.EnvironmentVariables["services__webfrontend__http__0"] = proxy.Urls.Single();
            }));
        }

        await using var app = await appHost.BuildAsync(token);
        await app.StartAsync(token);
        foreach (var resource in new[] { "webfrontend", "consumer-one", "consumer-two" })
        {
            await app.ResourceNotifications.WaitForResourceHealthyAsync(resource, token);
        }

        using var web = app.CreateHttpClient("webfrontend", "http");
        webReady.SetResult(web);
        using var producer = app.CreateHttpClient("producer", "http");
        using var home = await web.GetAsync("/", token);
        Assert.Equal(HttpStatusCode.OK, home.StatusCode);
        var competing = await PublishAsync(producer, "/sendMessage", token);
        var broadcast = await PublishAsync(producer, "/broadcastMessage", token);
        await WaitForReportsAsync(reports, competing, 1, token);
        await WaitForReportsAsync(reports, broadcast, 2, token);

        var commandService = app.Services.GetRequiredService<ResourceCommandService>();
        var stop = await commandService.ExecuteCommandAsync("webfrontend", "resource-stop", token);
        Assert.True(stop.Success, stop.Message);
        await app.ResourceNotifications.WaitForResourceAsync("webfrontend", KnownResourceStates.Finished, token);

        // Each consumer must handle a later delivery after its previous report has failed.
        for (var i = 0; i < 2; i++)
        {
            var offline = await PublishAsync(producer, "/broadcastMessage", token);
            await WaitForReportsAsync(reports, offline, 2, token);
            var offlineReports = reports.Where(report => report.Delivery.MessageId == offline).ToArray();
            Assert.All(offlineReports, report => Assert.True((int)report.Status >= 500, $"Unexpected status {report.Status}"));
            AssertFanout(offlineReports.Select(report => report.Delivery));
        }

        var shared = Assert.Single(reports, report => report.Delivery.MessageId == competing);
        Assert.Equal(HttpStatusCode.Accepted, shared.Status);
        Assert.Equal("CompetingConsumers", shared.Delivery.Pattern);
        Assert.Equal("business-events", shared.Delivery.Topic);
        Assert.Equal("business-events", shared.Delivery.Subscription);
        Assert.Contains(shared.Delivery.Consumer, new[] { "ConsumerOne", "ConsumerTwo" });
        var fanoutReports = reports.Where(report => report.Delivery.MessageId == broadcast).ToArray();
        Assert.All(fanoutReports, report => Assert.Equal(HttpStatusCode.Accepted, report.Status));
        AssertFanout(fanoutReports.Select(report => report.Delivery));
        Assert.All(reports, report => Assert.Equal(1, report.Delivery.DeliveryCount));
    }

    private static void AssertFanout(IEnumerable<DeliveryEvent> deliveries)
    {
        var ordered = deliveries.OrderBy(delivery => delivery.Consumer).ToArray();
        Assert.Collection(ordered,
            delivery => Assert.Equal(("ConsumerOne", "consumer-one"), (delivery.Consumer, delivery.Subscription)),
            delivery => Assert.Equal(("ConsumerTwo", "consumer-two"), (delivery.Consumer, delivery.Subscription)));
        Assert.All(ordered, delivery =>
        {
            Assert.Equal("PubSub", delivery.Pattern);
            Assert.Equal("broadcast-events", delivery.Topic);
        });
    }

    private static async Task<string> PublishAsync(HttpClient producer, string endpoint, CancellationToken token)
    {
        using var response = await producer.PostAsJsonAsync(endpoint, new { Message = "Order placed" }, token);
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var result = await response.Content.ReadFromJsonAsync<SendMessageResult>(token);
        return result!.MessageId;
    }

    private static async Task WaitForReportsAsync(ConcurrentQueue<(DeliveryEvent Delivery, HttpStatusCode Status)> reports,
        string id, int count, CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        try
        {
            while (reports.Count(report => report.Delivery.MessageId == id) < count)
            {
                await Task.Delay(100, deadline.Token);
            }
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            Assert.Fail($"Expected {count} reports for {id}, got {reports.Count(report => report.Delivery.MessageId == id)}.");
        }
    }
}
