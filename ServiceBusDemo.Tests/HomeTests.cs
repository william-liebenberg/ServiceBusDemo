using System.Net.Http.Json;
using AngleSharp.Html.Dom;
using Bunit;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components.Pages;
using TestContext = Xunit.TestContext;

namespace ServiceBusDemo.Tests;

public class HomeTests
{
    private const string EnteredMessage = "  Order 42 was placed \t ";
    private const string TrimmedMessage = "Order 42 was placed";
    private const string Cards = ".delivery-card";
    private const string Highlighted = ".delivery-highlighted";
    private const string DeliveryStatus = "#delivery-status[role=status][aria-live=polite][aria-atomic=true] span";

    [Theory]
    [InlineData("#pattern-competing", "/sendMessage", "business-events")]
    [InlineData("#pattern-pubsub", "/broadcastMessage", "broadcast-events")]
    public async Task SuccessfulSendsRetainInputAndAddSeparateHistoryEntries(string patternSelector, string endpoint, string topic)
    {
        using var handler = new RecordingHandler(requestNumber => Accepted($"id-{requestNumber}", topic));
        using var client = CreateClient(handler);
        await using var context = CreateContext(client, new DeliveryFeed());
        var home = context.Render<Home>();

        var radio = home.Find(patternSelector);
        radio.Change(((IHtmlInputElement)radio).Value);
        home.Find("#message").Change(EnteredMessage);

        for (var send = 1; send <= 2; send++)
        {
            await home.Find("form").SubmitAsync();

            Assert.Equal(EnteredMessage, GetRenderedMessage(home));
            Assert.True(((IHtmlInputElement)home.Find(patternSelector)).IsChecked);
            Assert.Equal($"Message id-{send} published to '{topic}'.", home.Find(".alert-success[role=status]").TextContent);
        }

        Assert.Equal(new[] { (HttpMethod.Post, endpoint, TrimmedMessage), (HttpMethod.Post, endpoint, TrimmedMessage) }, handler.Requests);
        Assert.Equal(new[] { "id-2", "id-1" }, home.FindAll("tbody td:nth-child(4)").Select(cell => cell.TextContent));
        Assert.Equal(new[] { TrimmedMessage, TrimmedMessage }, home.FindAll("tbody td:nth-child(5)").Select(cell => cell.TextContent));
        Assert.All(home.FindAll("tbody td:last-child"), cell => Assert.Equal("None reported", cell.TextContent));
        Assert.Single(home.FindAll(".table-responsive[tabindex='0'][aria-describedby=sent-consumers-note] table"));
        Assert.Contains("Only retained delivery reports are shown", home.Find("#sent-consumers-note").TextContent);
    }

    [Fact]
    public async Task FailedSendRetainsInputAndReenablesFormWithoutAddingHistory()
    {
        using var handler = new RecordingHandler(_ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        {
            Content = new StringContent("Try again later.")
        });
        using var client = CreateClient(handler);
        await using var context = CreateContext(client, new DeliveryFeed());
        var home = context.Render<Home>();
        home.Find("#message").Change(EnteredMessage);

        await home.Find("form").SubmitAsync();

        Assert.Single(handler.Requests);
        Assert.Equal(EnteredMessage, GetRenderedMessage(home));
        Assert.Equal("Failed to send message: Producer API returned 503 (Service Unavailable): Try again later.",
            home.Find(".alert-danger[role=status]").TextContent);
        Assert.Empty(home.FindAll("tbody tr"));
        Assert.Empty(home.FindAll("fieldset[disabled], #message[disabled], button[type=submit][disabled]"));
    }

    [Theory]
    [InlineData(" \t ", 0, "Please enter a message.")]
    [InlineData("", 4097, "maximum length of 4096")]
    public async Task InvalidMessageRetainsInputAndDoesNotSend(string text, int extraLength, string validationMessage)
    {
        using var handler = new RecordingHandler(HttpResponseMessage (_) => throw new InvalidOperationException("Invalid input must not reach the producer."));
        using var client = CreateClient(handler);
        await using var context = CreateContext(client, new DeliveryFeed());
        var home = context.Render<Home>();
        var message = text + new string('x', extraLength);
        home.Find("#message").Change(message);

        await home.Find("form").SubmitAsync();

        Assert.Empty(handler.Requests);
        Assert.Equal(message, GetRenderedMessage(home));
        Assert.Contains(validationMessage, home.Find(".validation-message").TextContent);
        Assert.Empty(home.FindAll(".alert[role=status], tbody tr"));
    }

    [Theory]
    [InlineData("ConsumerOne", "ConsumerTwo")]
    [InlineData("ConsumerTwo", "ConsumerOne")]
    public async Task RendersInitialSnapshotAndLiveReportsInTheirLanes(string consumer, string otherConsumer)
    {
        using var client = new HttpClient();
        var feed = new DeliveryFeed();
        var competing = Delivery("order-42", consumer, "CompetingConsumers", "business-events");
        feed.Add(competing);
        feed.Add(Delivery("unknown", "ConsumerThree"));
        await using var context = CreateContext(client, feed);
        var home = context.Render<Home>();

        Assert.Empty(home.Find(DeliveryStatus).TextContent);
        Assert.Single(home.FindAll(Cards));
        Assert.Empty(home.FindAll($"#{otherConsumer} {Cards}"));
        Assert.Equal("Consumer One", home.Find("#ConsumerOne h3").TextContent);
        Assert.Equal("Consumer Two", home.Find("#ConsumerTwo h3").TextContent);
        var card = home.Find($"#{consumer} .pattern-competing");
        var receivedAt = competing.ReceivedAt.ToLocalTime();
        Assert.Equal("Competing consumers", card.QuerySelector(".delivery-pattern")!.TextContent);
        Assert.Equal(new[] { competing.Subscription, competing.MessageId, competing.Body, $"{receivedAt:G} (UTC{receivedAt:zzz})" },
            card.QuerySelectorAll("dd").Select(field => field.TextContent));
        Assert.Equal("Received (local time)", card.QuerySelectorAll("dt").Last().TextContent);
        Assert.Empty(card.QuerySelectorAll("b"));
        Assert.Equal(receivedAt.ToString("O"), card.QuerySelector("time")!.GetAttribute("datetime"));

        await Task.Run(() =>
        {
            feed.Add(Delivery("broadcast-42", "ConsumerOne"));
            feed.Add(Delivery("broadcast-42", "ConsumerTwo", subscription: "consumer-two"));
        }, TestContext.Current.CancellationToken);

        home.WaitForAssertion(() =>
        {
            Assert.Equal(3, home.FindAll(Cards).Count);
            foreach (var lane in new[] { "ConsumerOne", "ConsumerTwo" })
            {
                var broadcast = home.Find($"#{lane} .pattern-pubsub");
                Assert.Equal("Publish/subscribe", broadcast.QuerySelector(".delivery-pattern")!.TextContent);
                Assert.Equal("broadcast-42", broadcast.QuerySelectorAll("dd")[1].TextContent);
                Assert.Equal("1 other visible report for this message.",
                    home.Find($"#{broadcast.GetAttribute("aria-describedby")}").TextContent);
            }
        });
    }

    [Fact]
    public async Task CorrelatesReportsBeforeAndAfterSendWithoutDuplicatingRecipients()
    {
        var requestStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var response = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var handler = new RecordingHandler(_ =>
        {
            requestStarted.SetResult();
            return response.Task;
        });
        using var client = CreateClient(handler);
        var feed = new DeliveryFeed();
        await using var context = CreateContext(client, feed);
        var home = context.Render<Home>();
        home.Find("#message").Change(EnteredMessage);
        var sending = home.Find("form").SubmitAsync();
        await requestStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        feed.Add(Delivery("sent", "ConsumerOne"));
        home.WaitForAssertion(() =>
        {
            Assert.Single(home.FindAll(Cards));
            Assert.Empty(home.FindAll("tbody tr"));
        });

        response.SetResult(Accepted("sent"));
        await sending;
        Assert.Equal("Consumer One", Recipients(home));

        feed.Add(Delivery("sent", "ConsumerOne"));
        feed.Add(Delivery("sent", "ConsumerTwo"));
        feed.Add(Delivery("sent", "ConsumerThree"));
        home.WaitForAssertion(() =>
        {
            var cards = home.FindAll(Cards);
            Assert.Equal(3, cards.Count);
            Assert.Equal(3, cards.Select(card => card.GetAttribute("aria-describedby")).Distinct().Count());
            Assert.All(cards, card => Assert.Equal("2 other visible reports for this message.",
                home.Find($"#{card.GetAttribute("aria-describedby")}").TextContent));
            Assert.Equal("Consumer One, Consumer Two", Recipients(home));
        });

        for (var i = 0; i < 200; i++)
        {
            feed.Add(Delivery($"other-{i}", "ConsumerThree"));
        }
        home.WaitForAssertion(() =>
        {
            Assert.Empty(home.FindAll(Cards));
            Assert.Equal("None reported", Recipients(home));
        });
    }

    [Fact]
    public async Task MostRecentActiveInteractionHighlightsOnlyItsMatchingMessageId()
    {
        using var client = new HttpClient();
        var feed = new DeliveryFeed();
        feed.Add(Delivery("shared-id", "ConsumerOne"));
        feed.Add(Delivery("shared-id", "ConsumerTwo"));
        feed.Add(Delivery("other-id", "ConsumerTwo", "CompetingConsumers"));
        feed.Add(Delivery("unrelated-id", "ConsumerOne", "CompetingConsumers"));
        await using var context = CreateContext(client, feed);
        var home = context.Render<Home>();
        const string sharedCard = "#ConsumerOne .pattern-pubsub";
        const string otherCard = "#ConsumerTwo .pattern-competing";

        AssertHighlights();
        Assert.Equal("0", home.Find(sharedCard).GetAttribute("tabindex"));
        home.Find(otherCard).MouseEnter();
        AssertHighlights("other-id");
        home.Find(sharedCard).Focus();
        AssertHighlights("shared-id", "shared-id");
        home.Find(sharedCard).Blur();
        AssertHighlights("other-id");
        home.Find(sharedCard).Focus();
        home.Find(otherCard).MouseLeave();
        AssertHighlights("shared-id", "shared-id");
        home.Find(otherCard).MouseEnter();
        AssertHighlights("other-id");
        home.Find(otherCard).MouseLeave();
        AssertHighlights("shared-id", "shared-id");
        home.Find(sharedCard).Blur();
        AssertHighlights();

        void AssertHighlights(params string[] ids) => Assert.Equal(ids,
            home.FindAll(Highlighted).Select(card => card.QuerySelectorAll("dd")[1].TextContent).Order());
    }

    [Fact]
    public async Task AnnouncesArrivalsEvenAtCapacityAndDoesNotHighlightEvictedCard()
    {
        using var client = new HttpClient();
        var feed = new DeliveryFeed();
        var earliest = Delivery("repeated", "ConsumerOne");
        feed.Add(earliest);
        for (var i = 0; i < 199; i++)
        {
            feed.Add(Delivery($"other-{i}", "ConsumerThree"));
        }
        await using var context = CreateContext(client, feed);
        var home = context.Render<Home>();
        var card = Assert.Single(home.FindAll(Cards));
        var descriptionId = card.GetAttribute("aria-describedby");
        Assert.Equal("No other visible reports for this message.", home.Find($"#{descriptionId}").TextContent);
        card.Focus();
        Assert.Single(home.FindAll(Highlighted));

        feed.Add(earliest);
        home.WaitForAssertion(() =>
        {
            Assert.NotEqual(descriptionId, Assert.Single(home.FindAll(Cards)).GetAttribute("aria-describedby"));
            Assert.Empty(home.FindAll(Highlighted));
            Assert.Equal("1 new delivery report.", home.Find(DeliveryStatus).TextContent);
        });
    }

    [Fact]
    public async Task ClearUpdatesBothInstancesAndDisposingOneDoesNotStopTheOther()
    {
        using var firstHandler = new RecordingHandler(_ => Accepted("first"));
        using var secondHandler = new RecordingHandler(_ => Accepted("second"));
        using var firstClient = CreateClient(firstHandler);
        using var secondClient = CreateClient(secondHandler);
        var feed = new DeliveryFeed();
        await using var firstContext = CreateContext(firstClient, feed);
        await using var secondContext = CreateContext(secondClient, feed);
        var first = firstContext.Render<Home>();
        var second = secondContext.Render<Home>();

        first.Find("#message").Change("First tab message");
        second.Find("#message").Change("Second tab message");
        await first.Find("form").SubmitAsync();
        await second.Find("form").SubmitAsync();
        feed.Add(Delivery("first", "ConsumerOne"));
        feed.Add(Delivery("second", "ConsumerTwo"));
        first.WaitForAssertion(() => Assert.Equal("Consumer One", Recipients(first)));
        second.WaitForAssertion(() => Assert.Equal("Consumer Two", Recipients(second)));

        first.Find("section button").Click();
        first.WaitForAssertion(() => AssertCleared(first, "first"));
        second.WaitForAssertion(() => AssertCleared(second, "second"));

        await firstContext.DisposeComponentsAsync();
        var firstRenderCount = first.RenderCount;
        feed.Add(Delivery("second", "ConsumerOne"));
        second.WaitForAssertion(() =>
        {
            Assert.Single(second.FindAll(Cards));
            Assert.Equal("Consumer One", Recipients(second));
            Assert.Equal("1 new delivery report.", second.Find(DeliveryStatus).TextContent);
        });
        Assert.Equal(firstRenderCount, first.RenderCount);

        static void AssertCleared(IRenderedComponent<Home> home, string sentId)
        {
            Assert.Empty(home.FindAll(Cards));
            Assert.Equal(sentId, home.Find("tbody td:nth-child(4)").TextContent);
            Assert.Equal("None reported", Recipients(home));
            Assert.Equal("Delivery reports cleared.", home.Find(DeliveryStatus).TextContent);
        }
    }

    [Fact]
    public async Task NotificationAlreadyInFlightDoesNotRenderDisposedHome()
    {
        using var client = new HttpClient();
        var feed = new DeliveryFeed();
        var token = TestContext.Current.CancellationToken;
        var notificationStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var continueNotification = new ManualResetEventSlim();
        feed.Changed += () =>
        {
            notificationStarted.SetResult();
            Assert.True(continueNotification.Wait(TimeSpan.FromSeconds(5), token));
        };
        await using var context = CreateContext(client, feed);
        var home = context.Render<Home>();
        var reporting = Task.Run(() => feed.Add(Delivery("racing", "ConsumerOne")), token);
        try
        {
            await notificationStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), token);
            await context.DisposeComponentsAsync();
        }
        finally
        {
            continueNotification.Set();
            await reporting;
        }
        Assert.Equal(1, home.RenderCount);
    }

    private static string Recipients(IRenderedComponent<Home> home) => home.Find("tbody td:last-child").TextContent;

    private static DeliveryEvent Delivery(string id, string consumer, string pattern = "PubSub", string subscription = "consumer-one") =>
        new(id, pattern, consumer, pattern == "PubSub" ? "broadcast-events" : "business-events", subscription,
            "<b>Order placed</b>", 1, DateTimeOffset.UnixEpoch);

    private static BunitContext CreateContext(HttpClient client, DeliveryFeed feed)
    {
        var context = new BunitContext();
        context.Services.AddSingleton(new ProducerApiClient(client));
        context.Services.AddControlTestClients();
        context.Services.AddSingleton(feed);
        return context;
    }

    // bUnit exposes the value from Blazor's render tree as an attribute rather than a DOM property.
    private static string? GetRenderedMessage(IRenderedComponent<Home> home) => home.Find("textarea#message").GetAttribute("value");

    private static HttpResponseMessage Accepted(string id, string topic = "business-events") => new(HttpStatusCode.Accepted)
    {
        Content = JsonContent.Create(new SendMessageResult(id, topic))
    };

    private static HttpClient CreateClient(RecordingHandler handler) => new(handler, disposeHandler: false)
    {
        BaseAddress = new Uri("http://producer.test")
    };

    private sealed class RecordingHandler(Func<int, Task<HttpResponseMessage>> createResponse) : HttpMessageHandler
    {
        public RecordingHandler(Func<int, HttpResponseMessage> createResponse) : this(number => Task.FromResult(createResponse(number))) { }

        public List<(HttpMethod Method, string Path, string Message)> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = await request.Content!.ReadFromJsonAsync<SendMessageRequest>(cancellationToken);
            Requests.Add((request.Method, request.RequestUri!.AbsolutePath, body!.Message));
            return await createResponse(Requests.Count);
        }
    }
}
