using System.Net.Http.Json;
using System.Text.Json.Nodes;
using AngleSharp.Html.Dom;
using Bunit;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components.Pages;
using TestContext = Xunit.TestContext;

namespace ServiceBusDemo.Tests;

public class TopicFiltersTests
{
    [Theory]
    [InlineData(MessagingPattern.TopicFilters, "/routedMessage", "High", "EU", "High", "EU")]
    [InlineData(MessagingPattern.TopicFilters, "/routedMessage", null, "EU", null, "EU")]
    [InlineData(MessagingPattern.TopicFilters, "/routedMessage", "High", "", "High", null)]
    [InlineData(MessagingPattern.CompetingConsumers, "/sendMessage", "High", "EU", null, null)]
    [InlineData(MessagingPattern.PubSub, "/broadcastMessage", "High", "EU", null, null)]
    public async Task ClientUsesPatternRouteAndSerializesOnlyApplicableProperties(MessagingPattern pattern,
        string endpoint, string? priority, string? region, string? expectedPriority, string? expectedRegion)
    {
        using var handler = new RecordingHandler();
        using var http = CreateClient(handler);
        var client = new ProducerApiClient(http);
        var result = await client.SendMessageAsync("Order placed", pattern, TestContext.Current.CancellationToken, priority, region);
        Assert.Equal(new SendMessageResult("routed-id", "routed-events"), result);
        var request = Assert.Single(handler.Requests);
        Assert.Equal(endpoint, request.Path);
        Assert.Equal("Order placed", (string?)request.Body["message"]);
        AssertProperty("priority", expectedPriority);
        AssertProperty("region", expectedRegion);

        void AssertProperty(string name, string? expected)
        {
            Assert.Equal(expected is not null, request.Body.ContainsKey(name));
            Assert.Equal(expected, (string?)request.Body[name]);
        }
    }

    [Fact]
    public async Task HomeShowsRulesAndOnlyRetainedReportsForTheSentMessage()
    {
        using var handler = new RecordingHandler();
        using var http = CreateClient(handler);
        var feed = new DeliveryFeed();
        await using var context = CreateHomeContext(http, feed);
        var home = context.Render<Home>();
        SelectPattern(home, "topic-filters");

        Assert.Equal("Priority (optional)", home.Find("label[for=route-priority]").TextContent);
        Assert.Equal("Region (optional)", home.Find("label[for=route-region]").TextContent);
        var rulesRegion = home.Find(".table-responsive[role=region][tabindex='0'][aria-labelledby=routing-rules-caption]");
        Assert.Equal("Routed subscription rules", rulesRegion.QuerySelector("#routing-rules caption#routing-rules-caption")?.TextContent);
        Assert.Equal(new[] { "high-priority", "eu-orders", "all" }, home.FindAll("#routing-rules tbody th").Select(cell => cell.TextContent));
        Assert.Equal(new[] { "Priority = 'High'", "Region = 'EU'" }, home.FindAll("#routing-rules td code").Select(cell => cell.TextContent));
        Assert.Contains("Unfiltered", home.Find("#routing-rules").TextContent);
        Assert.Contains("Subscription depth is unavailable for Topic filters (routed-events)", home.Find("#depth-note").TextContent);
        Assert.Contains("unavailable", home.Find("#burst-unavailable").TextContent);
        Assert.True(home.Find("#send-burst").HasAttribute("disabled"));
        Assert.True(home.Find(".burst-inputs").HasAttribute("disabled"));
        home.Find("#message").Change("Order placed");
        home.Find("#route-priority").Change("High");
        home.Find("#route-region").Change("EU");
        home.Find("#advanced-subject").Input("Orders");
        await home.Find("#burst-form").SubmitAsync();
        Assert.Empty(handler.Requests);
        await home.Find("form").SubmitAsync();
        var request = Assert.Single(handler.Requests);
        Assert.Equal("/routedMessage", request.Path);
        Assert.Equal("High", (string?)request.Body["priority"]);
        Assert.Equal("EU", (string?)request.Body["region"]);
        Assert.Equal("Orders", (string?)request.Body["subject"]);
        Assert.False(request.Body.ContainsKey("applicationProperties"));

        string Subscriptions() => home.Find(".reported-subscriptions").TextContent;
        Assert.Equal("None reported", Subscriptions());
        feed.Add(Report("other-message", "ConsumerTwo", "eu-orders"));
        feed.Add(Report("routed-id", "ConsumerOne", "all"));
        feed.Add(Report("routed-id", "ConsumerOne", "high-priority"));
        feed.Add(Report("routed-id", "ConsumerOne", "high-priority"));
        home.WaitForAssertion(() => Assert.Equal("all, high-priority", Subscriptions()));
        feed.Add(Report("routed-id", "ConsumerTwo", "eu-orders"));
        home.WaitForAssertion(() => Assert.Equal("all, eu-orders, high-priority", Subscriptions()));
        Assert.Equal(3, home.FindAll("#ConsumerOne .pattern-topic-filters").Count);
        Assert.Equal(2, home.FindAll("#ConsumerTwo .pattern-topic-filters").Count);

        for (var i = 0; i < 200; i++)
        {
            feed.Add(Report($"unrelated-{i}", "ConsumerOne", "all"));
        }
        home.WaitForAssertion(() => Assert.Equal("None reported", Subscriptions()));
        feed.Add(Report("routed-id", "ConsumerTwo", "eu-orders"));
        home.WaitForAssertion(() => Assert.Equal("eu-orders", Subscriptions()));
        home.Find(".deliveries button").Click();
        home.WaitForAssertion(() => Assert.Equal("None reported", Subscriptions()));
        Assert.Contains("does not prove non-delivery", home.Find("#sent-consumers-note").TextContent);
        Assert.Empty(home.FindAll(".delivery-card"));
    }

    [Fact]
    public async Task HomeKeepsTheSubmittedPropertiesForEachRoutedMessage()
    {
        using var handler = new RecordingHandler();
        using var http = CreateClient(handler);
        var feed = new DeliveryFeed();
        await using var context = CreateHomeContext(http, feed);
        var home = context.Render<Home>();
        SelectPattern(home, "topic-filters");

        home.Find("#message").Change("Order placed");
        home.Find("#route-priority").Change("High");
        await home.Find("form").SubmitAsync();
        home.Find("#route-priority").Change("");
        home.Find("#route-region").Change("EU");
        await home.Find("form").SubmitAsync();
        home.Find("#route-region").Change("US");

        Assert.False(handler.Requests[0].Body.ContainsKey("region"));
        Assert.False(handler.Requests[1].Body.ContainsKey("priority"));
        feed.Add(Report("routed-id", "ConsumerOne", "high-priority"));
        feed.Add(Report("routed-id-2", "ConsumerTwo", "eu-orders"));
        home.WaitForAssertion(() => Assert.Equal(new[] { "eu-orders", "high-priority" },
            home.FindAll(".reported-subscriptions").Select(cell => cell.TextContent)));
        Assert.Equal(new[] { "Not set", "\"EU\"", "\"High\"", "Not set" },
            home.FindAll(".sent-routing-properties dd").Select(value => value.TextContent.Trim()));

        home.Find(".deliveries button").Click();
        Assert.Equal(new[] { "Not set", "\"EU\"", "\"High\"", "Not set" },
            home.FindAll(".sent-routing-properties dd").Select(value => value.TextContent.Trim()));
    }

    [Fact]
    public async Task SwitchingPatternsDoesNotSendRetainedAdvancedPriorityOrRegionToRoutedTopic()
    {
        using var handler = new RecordingHandler();
        using var http = CreateClient(handler);
        await using var context = CreateHomeContext(http);
        var home = context.Render<Home>();
        home.Find("#message").Change("Order placed");
        home.Find("#advanced-heading").Click();
        home.Find("#advanced-priority").Change("High");
        home.Find("#advanced-region").Input("EU");
        home.Find("#advanced-heading").Click();
        SelectPattern(home, "topic-filters");

        Assert.Empty(home.FindAll("#advanced-priority, #advanced-region"));
        await home.Find("form").SubmitAsync();
        var routed = Assert.Single(handler.Requests);
        Assert.Equal("/routedMessage", routed.Path);
        Assert.False(routed.Body.ContainsKey("priority"));
        Assert.False(routed.Body.ContainsKey("region"));
        Assert.False(routed.Body.ContainsKey("applicationProperties"));
        AssertRoutingPropertiesNotSet(home);

        SelectPattern(home, "pubsub");
        await home.Find("form").SubmitAsync();
        var broadcast = handler.Requests[1];
        Assert.Equal("/broadcastMessage", broadcast.Path);
        Assert.Equal("High", (string?)broadcast.Body["applicationProperties"]?["Priority"]);
        Assert.Equal("EU", (string?)broadcast.Body["applicationProperties"]?["Region"]);
    }

    [Theory]
    [InlineData("Priority", "priority", "High")]
    [InlineData("Region", "region", "EU")]
    public async Task HomeReservesExactRoutingKeysButKeepsDifferentlyCasedCustomMetadata(string routingKey, string customKey, string value)
    {
        using var handler = new RecordingHandler();
        using var http = CreateClient(handler);
        await using var context = CreateHomeContext(http);
        var home = context.Render<Home>();
        SelectPattern(home, "topic-filters");
        home.Find("#message").Change("Order placed");
        home.Find("#advanced-heading").Click();
        home.Find("#add-property").Click();
        home.Find("#property-key-1").Input(routingKey);
        home.Find("#property-value-1").Input(value);

        await home.Find("form").SubmitAsync();
        Assert.Empty(handler.Requests);
        Assert.Contains($"set {routingKey} in Routing properties", home.Find("#advanced-errors").TextContent);

        home.Find("#property-key-1").Input(customKey);
        await home.Find("form").SubmitAsync();
        var request = Assert.Single(handler.Requests);
        Assert.False(request.Body.ContainsKey(customKey));
        var properties = Assert.IsType<JsonObject>(request.Body["applicationProperties"]);
        var property = Assert.Single(properties);
        Assert.Equal(customKey, property.Key);
        Assert.Equal(value, (string?)property.Value);
        Assert.Contains(customKey, home.Find(".sent-metadata").TextContent);
        AssertRoutingPropertiesNotSet(home);
    }

    private static void SelectPattern(IRenderedComponent<Home> home, string pattern)
    {
        var radio = (IHtmlInputElement)home.Find($"#pattern-{pattern}");
        radio.Change(radio.Value);
    }

    private static void AssertRoutingPropertiesNotSet(IRenderedComponent<Home> home) =>
        Assert.Equal(new[] { "Not set", "Not set" }, home.FindAll(".sent-routing-properties dd").Select(value => value.TextContent.Trim()));

    private static BunitContext CreateHomeContext(HttpClient http, DeliveryFeed? feed = null)
    {
        var context = new BunitContext();
        context.Services.AddSingleton(feed ?? new DeliveryFeed());
        context.Services.AddSingleton(new ProducerApiClient(http));
        context.Services.AddControlTestClients();
        context.JSInterop.SetupVoid("Blazor._internal.domWrapper.focus", _ => true).SetVoidResult();
        return context;
    }

    private static DeliveryEvent Report(string id, string consumer, string subscription) =>
        new(id, "TopicFilters", consumer, "routed-events", subscription, "Order placed", 1, DateTimeOffset.UnixEpoch,
            DeliveryOutcomes.Completed, DateTimeOffset.UnixEpoch.AddMinutes(1), DateTimeOffset.UnixEpoch);

    private static HttpClient CreateClient(RecordingHandler handler) => new(handler, disposeHandler: false)
    {
        BaseAddress = new Uri("http://producer.test")
    };

    private sealed class RecordingHandler : HttpMessageHandler
    {
        public List<(string Path, JsonObject Body)> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Assert.Equal(HttpMethod.Post, request.Method);
            var body = await request.Content!.ReadFromJsonAsync<JsonObject>(cancellationToken);
            Requests.Add((request.RequestUri!.AbsolutePath, body!));
            var id = Requests.Count == 1 ? "routed-id" : $"routed-id-{Requests.Count}";
            return new HttpResponseMessage(HttpStatusCode.Accepted)
            {
                Content = JsonContent.Create(new SendMessageResult(id, "routed-events"))
            };
        }
    }
}
