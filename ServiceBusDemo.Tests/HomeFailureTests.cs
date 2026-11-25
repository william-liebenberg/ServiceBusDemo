using System.Net.Http.Json;
using Bunit;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components.Pages;

namespace ServiceBusDemo.Tests;

public class HomeFailureTests
{
    [Fact]
    public async Task SingleAndBurstSendsUseEnabledOptionsAndRejectInvalidNumbers()
    {
        await using var context = new BunitContext();
        context.JSInterop.Mode = JSRuntimeMode.Loose;
        using var handler = new Handler();
        using var client = new HttpClient(handler) { BaseAddress = new Uri("https://producer.test") };
        context.Services.AddSingleton(new ProducerApiClient(client));
        context.Services.AddControlTestClients();
        context.Services.AddSingleton(new DeliveryFeed());
        var home = context.Render<Home>();
        Assert.Empty(home.FindAll("#fail-first-count, #delay-seconds"));
        foreach (var option in new[] { "fail-first", "always-fail", "slow-handler" })
        {
            Assert.NotEmpty(home.Find($"label[for='{option}']").TextContent);
            Assert.Null(home.Find($"#{option}").GetAttribute("checked"));
            home.Find($"#{option}").Change(true);
        }
        Assert.Equal("2", home.Find("#fail-first-count").GetAttribute("value"));
        Assert.Equal("5", home.Find("#delay-seconds").GetAttribute("value"));
        Assert.Contains("idempotent", home.Find("#failure-lab-help").TextContent);
        home.Find("#message").Change("  Order  ");
        home.Find("#fail-first-count").Input("1.5");
        await home.Find("form:first-of-type").SubmitAsync();
        Assert.Empty(handler.Requests);
        Assert.Equal("true", home.Find("#fail-first-count").GetAttribute("aria-invalid"));
        Assert.Contains("whole number", home.Find("#fail-first-error").TextContent);
        home.Find("#fail-first-count").Input("3");
        await home.Find("form:first-of-type").SubmitAsync();
        Assert.Equal(new("Order", 3, true, 5), Assert.Single(handler.Requests));

        home.Find("#burst-count").Input("2");
        home.Find("#delay-seconds").Input("121");
        await home.Find("#burst-form").SubmitAsync();
        Assert.Single(handler.Requests);
        Assert.Equal("true", home.Find("#delay-seconds").GetAttribute("aria-invalid"));
        home.Find("#delay-seconds").Input("9");
        home.Find("#always-fail").Change(false);
        await home.Find("#burst-form").SubmitAsync();
        Assert.Equal(3, handler.Requests.Count);
        Assert.All(handler.Requests.Skip(1), request => Assert.Equal(new("Order", 3, false, 9), request));
    }

    [Fact]
    public async Task EmptyFeedExplainsWhenCardsAppearAndWhyTheyMayBeMissing()
    {
        await using var context = new BunitContext();
        using var client = new HttpClient();
        context.Services.AddSingleton(new ProducerApiClient(client));
        context.Services.AddControlTestClients();
        context.Services.AddSingleton(new DeliveryFeed());

        var home = context.Render<Home>();

        Assert.Empty(home.FindAll(".delivery-card"));
        var help = home.Find(".deliveries[aria-describedby='delivery-report-help'] #delivery-report-help").TextContent;
        Assert.Contains("only after an attempt is settled", help);
        Assert.Contains("won't appear while its delay is still running", help);
        Assert.Contains("Reports can be lost; a missing card doesn't mean settlement failed", help);
    }

    [Fact]
    public async Task SeparateAttemptCardsShowCountsAndOutcomes()
    {
        await using var context = new BunitContext();
        using var handler = new Handler();
        using var client = new HttpClient(handler);
        context.Services.AddSingleton(new ProducerApiClient(client));
        context.Services.AddControlTestClients();
        var feed = new DeliveryFeed();
        context.Services.AddSingleton(feed);
        var received = DateTimeOffset.UnixEpoch;
        var first = new DeliveryEvent("same-id", "CompetingConsumers", "ConsumerOne", "business-events", "business-events",
            "Order", 1, received, DeliveryOutcomes.Abandoned, received.AddMinutes(1), received.AddSeconds(5));
        feed.Add(first);
        feed.Add(first with { DeliveryCount = 2, Outcome = DeliveryOutcomes.Completed });
        var home = context.Render<Home>();
        Assert.Collection(home.FindAll(".delivery-card"),
            card => Verify(card, "2", DeliveryOutcomes.Completed),
            card => Verify(card, "1", DeliveryOutcomes.Abandoned));

        static void Verify(AngleSharp.Dom.IElement card, string count, string outcome)
        {
            Assert.Equal(new[] { count, outcome }, card.QuerySelectorAll("dd").Skip(4).Take(2).Select(field => field.TextContent));
            Assert.Contains("1 other visible report", card.TextContent);
        }
    }

    private sealed class Handler : HttpMessageHandler
    {
        public List<SendMessageRequest> Requests { get; } = [];
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add((await request.Content!.ReadFromJsonAsync<SendMessageRequest>(cancellationToken))!);
            return new(HttpStatusCode.Accepted) { Content = JsonContent.Create(new SendMessageResult($"id-{Requests.Count}", "business-events")) };
        }
    }
}
