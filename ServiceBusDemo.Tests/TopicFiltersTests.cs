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
        await using var context = new BunitContext();
        var feed = new DeliveryFeed();
        context.Services.AddSingleton(feed);
        context.Services.AddSingleton(new ProducerApiClient(http));
        context.Services.AddControlTestClients();
        var home = context.Render<Home>();
        var radio = (IHtmlInputElement)home.Find("#pattern-topic-filters");
        radio.Change(radio.Value);

        Assert.Equal("Priority (optional)", home.Find("label[for=route-priority]").TextContent);
        Assert.Equal("Region (optional)", home.Find("label[for=route-region]").TextContent);
        var rulesRegion = home.Find(".table-responsive[role=region][tabindex='0'][aria-labelledby=routing-rules-caption]");
        Assert.Equal("Routed subscription rules", rulesRegion.QuerySelector("#routing-rules caption#routing-rules-caption")?.TextContent);
        Assert.Equal(new[] { "high-priority", "eu-orders", "all" }, home.FindAll("#routing-rules tbody th").Select(cell => cell.TextContent));
        Assert.Equal(new[] { "Priority = 'High'", "Region = 'EU'" }, home.FindAll("#routing-rules td code").Select(cell => cell.TextContent));
        Assert.Contains("Unfiltered", home.Find("#routing-rules").TextContent);
        Assert.Contains("unavailable", home.Find("#burst-unavailable").TextContent);
        Assert.True(home.Find("#send-burst").HasAttribute("disabled"));
        Assert.True(home.Find(".burst-inputs").HasAttribute("disabled"));
        home.Find("#message").Change("Order placed");
        home.Find("#route-priority").Change("High");
        home.Find("#route-region").Change("EU");
        home.Find("#advanced-subject").Input("Orders");
        home.Find("#advanced-region").Input("West");
        await home.Find("#burst-form").SubmitAsync();
        Assert.Empty(handler.Requests);
        await home.Find("form").SubmitAsync();
        var request = Assert.Single(handler.Requests);
        Assert.Equal("/routedMessage", request.Path);
        Assert.Equal("High", (string?)request.Body["priority"]);
        Assert.Equal("EU", (string?)request.Body["region"]);
        Assert.Equal("Orders", (string?)request.Body["subject"]);
        Assert.Equal("West", (string?)request.Body["applicationProperties"]?["Region"]);

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
        await using var context = new BunitContext();
        var feed = new DeliveryFeed();
        context.Services.AddSingleton(feed);
        context.Services.AddSingleton(new ProducerApiClient(http));
        context.Services.AddControlTestClients();
        var home = context.Render<Home>();
        var radio = (IHtmlInputElement)home.Find("#pattern-topic-filters");
        radio.Change(radio.Value);

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

    private static DeliveryEvent Report(string id, string consumer, string subscription) =>
        new(id, "TopicFilters", consumer, "routed-events", subscription, "Order placed", 1, DateTimeOffset.UnixEpoch);

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
