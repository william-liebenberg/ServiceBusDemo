using System.Net.Http.Json;
using AngleSharp.Html.Dom;
using Bunit;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components.Pages;

namespace ServiceBusDemo.Tests;

public class HomeDepthTests
{
    [Fact]
    public async Task ShowsEachSubscriptionOnceBeforeSending()
    {
        await using var context = CreateContext(new DeliveryFeed());
        var home = context.Render<Home>();
        var rows = home.FindAll("section[aria-labelledby=depth-heading] .depth-row");
        Assert.Collection(rows,
            row => Assert.Equal(new[] { "business-events", "business-events" }, row.QuerySelectorAll("code").Select(code => code.TextContent)),
            row => Assert.Equal(new[] { "broadcast-events", "consumer-one" }, row.QuerySelectorAll("code").Select(code => code.TextContent)),
            row => Assert.Equal(new[] { "broadcast-events", "consumer-two" }, row.QuerySelectorAll("code").Select(code => code.TextContent)));
        Assert.All(rows, row => Assert.Contains("Dead-letter: unavailable", row.TextContent));
        AssertDepth(home, 0, 0, 0);
    }

    [Fact]
    public async Task PausingAndResumingConsumersShowsSharedAndBroadcastBacklogs()
    {
        var feed = new DeliveryFeed();
        var oneState = (int)ConsumerState.Running;
        var twoState = (int)ConsumerState.Running;
        await using var context = CreateContext(feed, configureControls: (request, _) =>
        {
            var one = request.RequestUri!.Host == "consumer-one.test";
            ref var state = ref (one ? ref oneState : ref twoState);
            if (request.Method == HttpMethod.Post)
            {
                Volatile.Write(ref state, (int)(request.RequestUri.AbsolutePath switch
                {
                    "/control/pause" => ConsumerState.Paused,
                    "/control/resume" => ConsumerState.Running,
                    _ => throw new InvalidOperationException("Unexpected control action.")
                }));
            }
            return Task.FromResult(ControlTestClients.Reply(one ? "ConsumerOne" : "ConsumerTwo", (ConsumerState)Volatile.Read(ref state)));
        });
        var home = context.Render<Home>();
        home.Find("#message").Change("Order 42");
        home.WaitForAssertion(() => Assert.Equal(new[] { "Running", "Running" }, States(home)));
        await home.Find("#ConsumerOne .consumer-pause").ClickAsync(new());
        await home.Find("#ConsumerTwo .consumer-pause").ClickAsync(new());
        Assert.Equal(new[] { "Paused", "Paused" }, States(home));

        home.Find("#burst-count").Input("2");
        await home.Find("#burst-form").SubmitAsync();
        AssertDepth(home, 2, 0, 0);

        await home.Find("#ConsumerOne .consumer-resume").ClickAsync(new());
        Assert.Equal(new[] { "Running", "Paused" }, States(home));
        // Neither an unrelated ID nor a matching ID on the wrong topic/subscription reduces depth.
        Report(feed, "external");
        Report(feed, "1", topic: "other");
        Report(feed, "1", subscription: "other");
        AssertDepth(home, 2, 0, 0);
        Report(feed, "1");
        Report(feed, "1", consumer: "ConsumerTwo");
        home.WaitForAssertion(() => AssertDepth(home, 1, 0, 0));
        Report(feed, "2");
        home.WaitForAssertion(() => AssertDepth(home, 0, 0, 0));

        var radio = (IHtmlInputElement)home.Find("#pattern-pubsub");
        radio.Change(radio.Value);
        await home.Find("form").SubmitAsync();
        AssertDepth(home, 0, 1, 1);
        Report(feed, "3", "broadcast-events", "consumer-one");
        home.WaitForAssertion(() => AssertDepth(home, 0, 0, 1));
        await home.Find("#ConsumerTwo .consumer-resume").ClickAsync(new());
        Assert.Equal(new[] { "Running", "Running" }, States(home));
        Report(feed, "3", "broadcast-events", "consumer-two", "ConsumerTwo");
        home.WaitForAssertion(() => AssertDepth(home, 0, 0, 0));
    }

    [Fact]
    public async Task UsesRetainedEarlyReportsAndKeepsAcknowledgementsLocalToThePage()
    {
        var feed = new DeliveryFeed();
        var release = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var context = CreateContext(feed, (_, token) => release.Task.WaitAsync(token));
        await using var otherContext = CreateContext(feed);
        var home = context.Render<Home>();
        var other = otherContext.Render<Home>();
        home.Find("#message").Change("Order 42");
        var sending = home.Find("form").SubmitAsync();
        await home.InvokeAsync(() => Report(feed, "early"));
        AssertDepth(home, 0, 0, 0);
        release.SetResult(Accepted("early", "business-events"));
        await sending.WaitAsync(TimeSpan.FromSeconds(5), Xunit.TestContext.Current.CancellationToken);
        Assert.Contains("early", home.Find("tbody").TextContent);
        AssertDepth(home, 0, 0, 0);

        await home.InvokeAsync(() =>
        {
            for (var i = 0; i < 199; i++) Report(feed, $"external-{i}");
        });
        AssertDepth(home, 0, 0, 0);
        Report(feed, "evicts-early");
        home.WaitForAssertion(() => AssertDepth(home, 1, 0, 0));
        AssertDepth(other, 0, 0, 0);

        Report(feed, "early");
        home.WaitForAssertion(() => AssertDepth(home, 0, 0, 0));
        home.Find(".deliveries button").Click();
        AssertDepth(home, 1, 0, 0);
        AssertDepth(other, 0, 0, 0);
    }

    [Fact]
    public async Task FailedSendDoesNotChangeDepthAndNextSendCanSucceed()
    {
        var requests = 0;
        await using var context = CreateContext(new DeliveryFeed(), (_, _) => Task.FromResult(
            ++requests == 1 ? new HttpResponseMessage(HttpStatusCode.ServiceUnavailable) : Accepted("recovered", "business-events")));
        var home = context.Render<Home>();
        home.Find("#message").Change("Order 42");
        await home.Find("form").SubmitAsync();
        Assert.Contains("Failed to send", home.Find(".alert-danger").TextContent);
        AssertDepth(home, 0, 0, 0);
        await home.Find("form").SubmitAsync();
        AssertDepth(home, 1, 0, 0);
    }

    private static void AssertDepth(IRenderedComponent<Home> home, params int[] expected) =>
        Assert.Equal(expected.Select(count => $"Estimated pending: {count}"), home.FindAll(".depth-pending").Select(row => row.TextContent.Trim()));

    private static IEnumerable<string> States(IRenderedComponent<Home> home) =>
        home.FindAll(".consumer-state").Select(state => state.TextContent.Trim());

    private static void Report(DeliveryFeed feed, string id, string topic = "business-events",
        string subscription = "business-events", string consumer = "ConsumerOne") =>
        feed.Add(new DeliveryEvent(id, topic == "broadcast-events" ? "PubSub" : "CompetingConsumers", consumer,
            topic, subscription, "Order 42", 1, DateTimeOffset.UnixEpoch));

    private static HttpResponseMessage Accepted(string id, string topic) =>
        new(HttpStatusCode.Accepted) { Content = JsonContent.Create(new SendMessageResult(id, topic)) };

    private static BunitContext CreateContext(DeliveryFeed feed,
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? send = null,
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? configureControls = null)
    {
        var context = new BunitContext();
        var nextId = 0;
        context.Services.AddHttpClient<ProducerApiClient>(http => http.BaseAddress = new Uri("http://producer.test"))
            .ConfigurePrimaryHttpMessageHandler(() => new ControlTestClients.Handler(send ?? ((request, _) =>
                Task.FromResult(Accepted((++nextId).ToString(),
                    request.RequestUri!.AbsolutePath == "/broadcastMessage" ? "broadcast-events" : "business-events")))));
        context.Services.AddControlTestClients(configureControls);
        context.Services.AddSingleton(feed);
        context.JSInterop.SetupVoid("Blazor._internal.domWrapper.focus", _ => true).SetVoidResult();
        context.JSInterop.SetupVoid("Blazor._internal.NavigationLock.enableNavigationPrompt", _ => true).SetVoidResult();
        context.JSInterop.SetupVoid("Blazor._internal.NavigationLock.disableNavigationPrompt", _ => true).SetVoidResult();
        return context;
    }
}
