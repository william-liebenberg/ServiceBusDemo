using System.Collections.Concurrent;
using System.Net.Http.Json;
using AngleSharp.Html.Dom;
using Aspire.Hosting;
using Bunit;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components.Pages;
using TestContext = Xunit.TestContext;

namespace ServiceBusDemo.Tests;

public class WebTests
{
    [Fact]
    public async Task ConsumersReportAllPatternsAndKeepProcessingWhenWebStops()
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        var reports = new ConcurrentQueue<(DeliveryEvent Delivery, HttpStatusCode Status)>();
        var observedFeed = new DeliveryFeed();
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
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
            {
                status = HttpStatusCode.GatewayTimeout;
            }
            reports.Enqueue((delivery, status));
            if (status == HttpStatusCode.Accepted)
            {
                observedFeed.Add(delivery);
            }
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

        // Capture the same discovery entries Aspire supplies to the running Web project.
        var webDiscovery = new TaskCompletionSource<KeyValuePair<string, object>[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        appHost.Resources.Single(resource => resource.Name == "webfrontend").Annotations.Add(
            new EnvironmentCallbackAnnotation(context =>
            {
                webDiscovery.TrySetResult(context.EnvironmentVariables
                    .Where(entry => entry.Key.StartsWith("services__", StringComparison.OrdinalIgnoreCase))
                    .ToArray());
            }));

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

        await VerifyRoutingAsync(producer, reports, token);
        await VerifyBurstsAsync(producer, observedFeed, token);
        var discovery = new Dictionary<string, string?>();
        foreach (var (key, value) in await webDiscovery.Task.WaitAsync(token))
        {
            discovery.Add(key.Replace("__", ":"), value is IValueProvider provider
                ? await provider.GetValueAsync(token)
                : (string)value);
        }
        // Use the real Web registrations and discovery handlers against the Aspire resources,
        // without adding a test control endpoint to the running Web app.
        await using var webFactory = new WebApplicationFactory<DeliveryFeed>().WithWebHostBuilder(builder =>
            builder.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(discovery)));
        var one = webFactory.Services.GetRequiredService<ConsumerOneControlClient>();
        var two = webFactory.Services.GetRequiredService<ConsumerTwoControlClient>();
        await VerifyConsumerControlAsync(producer, one, two, reports, token);

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

    private static async Task VerifyRoutingAsync(HttpClient producer,
        ConcurrentQueue<(DeliveryEvent Delivery, HttpStatusCode Status)> reports, CancellationToken token)
    {
        const string body = "Routed order";
        (string? Priority, string? Region, string[] Expected)[] cases =
        [
            ("High", null, ["all", "high-priority"]),
            (null, "EU", ["all", "eu-orders"]),
            ("High", "EU", ["all", "eu-orders", "high-priority"]),
            ("Normal", "US", ["all"]),
            (null, null, ["all"])
        ];
        var sends = new List<(string Id, string[] Expected)>();
        var api = new ProducerApiClient(producer);
        foreach (var (priority, region, expected) in cases)
        {
            var result = await api.SendMessageAsync(body, MessagingPattern.TopicFilters, token, priority, region);
            Assert.Equal("routed-events", result.Topic);
            sends.Add((result.MessageId, expected));
        }
        foreach (var (id, expected) in sends)
        {
            await WaitForReportsAsync(reports, id, expected.Length, token);
        }

        // Also allow incorrectly matching subscriptions time to report before checking the exact sets.
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        foreach (var (id, expected) in sends)
        {
            var copies = reports.Where(report => report.Delivery.MessageId == id).ToArray();
            Assert.Equal(expected, copies.Select(copy => copy.Delivery.Subscription).Order(StringComparer.Ordinal));
            Assert.All(copies, copy =>
            {
                Assert.Equal(HttpStatusCode.Accepted, copy.Status);
                Assert.Equal(("TopicFilters", "routed-events", body), (copy.Delivery.Pattern, copy.Delivery.Topic, copy.Delivery.Body));
                Assert.Equal(copy.Delivery.Subscription == "eu-orders" ? "ConsumerTwo" : "ConsumerOne", copy.Delivery.Consumer);
            });
        }
    }

    private static async Task VerifyConsumerControlAsync(HttpClient producer, ConsumerOneControlClient one,
        ConsumerTwoControlClient two, ConcurrentQueue<(DeliveryEvent Delivery, HttpStatusCode Status)> reports, CancellationToken token)
    {
        Assert.Equal(ConsumerState.Paused, (await two.PauseAsync(token)).State);
        Assert.Equal(ConsumerState.Running, (await one.GetStatusAsync(token)).State);
        var failover = new[] { await PublishAsync(producer, "/sendMessage", token), await PublishAsync(producer, "/sendMessage", token) };
        foreach (var id in failover)
        {
            await WaitForReportsAsync(reports, id, 1, token);
            AssertFromOne(id);
        }

        Assert.Equal(ConsumerState.Paused, (await one.PauseAsync(token)).State);
        Assert.Equal(ConsumerState.Paused, (await two.GetStatusAsync(token)).State);
        var backlog = new[] { await PublishAsync(producer, "/sendMessage", token), await PublishAsync(producer, "/sendMessage", token) };
        // Give any incorrectly active processor a chance to report before resuming.
        await Task.Delay(TimeSpan.FromSeconds(1), token);
        Assert.DoesNotContain(reports, report => backlog.Contains(report.Delivery.MessageId));
        Assert.Equal(ConsumerState.Running, (await one.ResumeAsync(token)).State);
        foreach (var id in backlog)
        {
            await WaitForReportsAsync(reports, id, 1, token);
            AssertFromOne(id);
        }

        var fanout = await PublishAsync(producer, "/broadcastMessage", token);
        await WaitForReportsAsync(reports, fanout, 1, token);
        AssertFromOne(fanout);
        Assert.Equal(ConsumerState.Paused, (await two.GetStatusAsync(token)).State);
        Assert.Equal(ConsumerState.Running, (await two.ResumeAsync(token)).State);
        await WaitForReportsAsync(reports, fanout, 2, token);
        var copies = reports.Where(report => report.Delivery.MessageId == fanout).ToArray();
        Assert.All(copies, copy => Assert.Equal(HttpStatusCode.Accepted, copy.Status));
        AssertFanout(copies.Select(copy => copy.Delivery));
        foreach (var id in failover.Concat(backlog))
        {
            AssertFromOne(id);
        }

        void AssertFromOne(string id)
        {
            var report = Assert.Single(reports, item => item.Delivery.MessageId == id);
            Assert.Equal(HttpStatusCode.Accepted, report.Status);
            Assert.Equal("ConsumerOne", report.Delivery.Consumer);
        }
    }

    private static async Task VerifyBurstsAsync(HttpClient producer, DeliveryFeed feed, CancellationToken token)
    {
        await using var context = new BunitContext();
        context.JSInterop.SetupVoid("Blazor._internal.domWrapper.focus", _ => true).SetVoidResult();
        context.JSInterop.SetupVoid("Blazor._internal.NavigationLock.enableNavigationPrompt", _ => true).SetVoidResult();
        context.JSInterop.SetupVoid("Blazor._internal.NavigationLock.disableNavigationPrompt", _ => true).SetVoidResult();
        context.Services.AddSingleton(new ProducerApiClient(producer));
        context.Services.AddControlTestClients();
        context.Services.AddSingleton(feed);
        var home = context.Render<Home>();
        home.Find("#message").Change("Aspire burst smoke test");
        home.Find("#burst-count").Input("3");

        await home.Find("#burst-form").SubmitAsync().WaitAsync(token);
        home.WaitForAssertion(() => Assert.Equal("Reported deliveries: 3 / 3 expected", home.Find("#burst-total").TextContent),
            TimeSpan.FromSeconds(15));

        var pubSub = home.Find("#pattern-pubsub");
        pubSub.Change(((IHtmlInputElement)pubSub).Value);
        await home.Find("#burst-form").SubmitAsync().WaitAsync(token);
        home.WaitForAssertion(() => Assert.Equal("Reported deliveries: 6 / 6 expected", home.Find("#burst-total").TextContent),
            TimeSpan.FromSeconds(15));
        Assert.Equal("3", home.Find("#burst-ConsumerOne").GetAttribute("value"));
        Assert.Equal("3", home.Find("#burst-ConsumerTwo").GetAttribute("value"));

        await home.Find("#burst-count").InputAsync(new() { Value = "20" });
        await home.Find("#burst-paced").ChangeAsync(new() { Value = true });
        var rate = home.WaitForElement("#burst-rate");
        await rate.InputAsync(new() { Value = "1" });
        var sending = home.Find("#burst-form").SubmitAsync();
        home.WaitForAssertion(() => Assert.InRange(int.Parse(home.Find("#burst-progress[max='20']").GetAttribute("value")!), 1, 19),
            TimeSpan.FromSeconds(15));
        await home.Find("#cancel-burst").ClickAsync(new()).WaitAsync(token);
        await sending.WaitAsync(token);
        var acknowledged = int.Parse(home.Find("#burst-progress").GetAttribute("value")!);
        Assert.InRange(acknowledged, 1, 19);
        Assert.Equal("Burst cancelled.", home.Find("#burst-status").TextContent);
        var expected = acknowledged * 2;
        home.WaitForAssertion(() => Assert.Equal($"Reported deliveries: {expected} / {expected} expected", home.Find("#burst-total").TextContent),
            TimeSpan.FromSeconds(15));
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
