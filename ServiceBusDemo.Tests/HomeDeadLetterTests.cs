using System.Net.Http.Json;
using Bunit;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components.Pages;
using TestContext = Xunit.TestContext;

namespace ServiceBusDemo.Tests;

public class HomeDeadLetterTests
{
    private static readonly DeadLetterSubscription[] Pairs = [new("business-events", "business-events"), new("routed-events", "eu-orders")];

    [Fact]
    public async Task TabShowsDetailsPagesAndActionResultsWithoutLosingMessagingState()
    {
        await using var context = new BunitContext();
        using var handler = new Handler();
        using var client = Client(context, handler);
        var home = context.Render<Home>();
        home.Find("#message").Change("keep this draft");
        Assert.Empty(handler.Requests);
        Assert.Equal("false", home.Find("#dead-letter-tab[role=tab]").GetAttribute("aria-selected"));
        await home.Find("#dead-letter-tab").ClickAsync(new());
        Assert.Equal("true", home.Find("#dead-letter-tab").GetAttribute("aria-selected"));
        Assert.Equal("dead-letter-tab", home.Find("#dead-letter-panel[role=tabpanel]").GetAttribute("aria-labelledby"));
        Assert.Equal("Topic / subscription", home.Find("label[for=dead-letter-subscription]").TextContent);
        Assert.Equal(2, home.FindAll("#dead-letter-subscription option").Count);
        await home.Find("#dead-letter-subscription").ChangeAsync("1");
        Assert.Contains("subscription=eu-orders", handler.Requests.Last());
        Assert.Contains("not a broker count", home.Find("#dead-letter-help").TextContent);
        var card = home.Find(".dead-letter-card").TextContent;
        foreach (var text in new[] { "same-id", "poison", "description", "body", "Priority", "High", "5" }) Assert.Contains(text, card);
        await home.Find("#dead-letter-next").ClickAsync(new());
        Assert.Contains("fromSequenceNumber=9", handler.Requests.Last());
        Assert.Contains("Sequence 9", home.Find(".dead-letter-card h3").TextContent);
        await home.Find("#dead-letter-previous").ClickAsync(new());
        Assert.DoesNotContain("fromSequenceNumber", handler.Requests.Last());
        await home.Find("[aria-label='Resubmit sequence 7']").ClickAsync(new());
        Assert.Contains(handler.Requests, request => request.StartsWith("POST /dead-letters/messages/7/resubmit"));
        Assert.StartsWith("GET /dead-letters/messages?", handler.Requests.Last());
        Assert.Contains("Published copy copy-id", home.Find("#dead-letter-status[role=status]").TextContent);
        await home.Find("[aria-label='Purge sequence 7']").ClickAsync(new());
        Assert.Contains(handler.Requests, request => request.StartsWith("POST /dead-letters/messages/7/purge"));
        Assert.Contains("Purge was not confirmed", home.Find("#dead-letter-action-error[role=alert]").TextContent);
        handler.PeekError = true;
        await home.Find("#dead-letter-refresh").ClickAsync(new());
        Assert.Contains("Emulator offline", home.Find("#dead-letter-load-error[role=alert]").TextContent);
        handler.PeekError = false;
        await home.Find("#dead-letter-refresh").ClickAsync(new());
        Assert.Empty(home.Find("#dead-letter-load-error").TextContent);
        home.Find("#messaging-tab").Click();
        Assert.Contains("keep this draft", home.Find("#message").OuterHtml);
    }

    [Fact]
    public async Task StaleSelectionAndPreActionPeeksCannotReplaceNewerResults()
    {
        var token = TestContext.Current.CancellationToken;
        await using var context = new BunitContext();
        using var handler = new Handler();
        using var client = Client(context, handler);
        var home = context.Render<Home>();
        await home.Find("#dead-letter-tab").ClickAsync(new());
        var first = handler.HoldNextPeek();
        var refresh = home.Find("#dead-letter-refresh").ClickAsync(new());
        await first.Entered.Task.WaitAsync(token);
        await home.Find("#dead-letter-subscription").ChangeAsync("1");
        first.Response.SetResult(Page(99));
        await refresh;
        Assert.Contains("Sequence 7", home.Find(".dead-letter-card h3").TextContent);

        var second = handler.HoldNextPeek();
        refresh = home.Find("#dead-letter-refresh").ClickAsync(new());
        await second.Entered.Task.WaitAsync(token);
        handler.EmptyPage = true;
        await home.Find("[aria-label='Resubmit sequence 7']").ClickAsync(new());
        second.Response.SetResult(Page(7));
        await refresh;
        Assert.Empty(home.FindAll(".dead-letter-card"));
        Assert.Contains("No dead letters", home.Find("#dead-letter-status").TextContent);
    }

    [Fact]
    public async Task PollsWhileActiveAndCancelsPendingPollOnDisposal()
    {
        var token = TestContext.Current.CancellationToken;
        await using var context = new BunitContext();
        using var handler = new Handler();
        using var client = Client(context, handler);
        var home = context.Render<Home>();
        await home.Find("#dead-letter-tab").ClickAsync(new());
        var pending = handler.HoldNextPeek();
        var requestToken = await pending.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5), token);
        Assert.False(requestToken.IsCancellationRequested);
        await context.DisposeComponentsAsync();
        Assert.True(requestToken.IsCancellationRequested);
        pending.Response.SetResult(Page(7));
    }

    private static HttpClient Client(BunitContext context, Handler handler)
    {
        context.JSInterop.Mode = JSRuntimeMode.Loose;
        var client = new HttpClient(handler) { BaseAddress = new Uri("https://producer.test") };
        context.Services.AddSingleton(new ProducerApiClient(client));
        context.Services.AddControlTestClients();
        context.Services.AddSingleton(new DeliveryFeed());
        return client;
    }

    private static HttpResponseMessage Page(long sequence, long? next = null) => Json(new DeadLetterPage(
        [new(sequence, "same-id", "poison", "description", "body", new Dictionary<string, string> { ["Priority"] = "High" }, 5)], next));

    private static HttpResponseMessage Json<T>(T value) => new(HttpStatusCode.OK) { Content = JsonContent.Create(value) };

    private sealed class PendingPeek
    {
        public TaskCompletionSource<CancellationToken> Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<HttpResponseMessage> Response { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class Handler : HttpMessageHandler
    {
        private PendingPeek? pending;
        public System.Collections.Concurrent.ConcurrentQueue<string> Requests { get; } = new();
        public bool PeekError { get; set; }
        public bool EmptyPage { get; set; }
        public PendingPeek HoldNextPeek()
        {
            var peek = new PendingPeek();
            Interlocked.Exchange(ref pending, peek);
            return peek;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.AbsolutePath;
            Requests.Enqueue($"{request.Method} {request.RequestUri.PathAndQuery}");
            if (path.EndsWith("/subscriptions")) return Task.FromResult(Json(Pairs));
            if (path.EndsWith("/resubmit")) return Task.FromResult(Json(new DeadLetterActionResult("copy-id")));
            if (path.EndsWith("/purge")) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.BadGateway) { Content = new StringContent("Purge was not confirmed") });
            if (Interlocked.Exchange(ref pending, null) is { } peek)
            {
                peek.Entered.SetResult(cancellationToken);
                return peek.Response.Task;
            }
            if (PeekError) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.BadGateway) { Content = new StringContent("Emulator offline") });
            if (EmptyPage) return Task.FromResult(Json(new DeadLetterPage([], null)));
            return Task.FromResult(request.RequestUri.Query.Contains("fromSequenceNumber=9") ? Page(9) : Page(7, 9));
        }
    }
}
