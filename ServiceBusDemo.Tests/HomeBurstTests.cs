using System.Diagnostics;
using System.Net.Http.Json;
using AngleSharp.Html.Dom;
using Bunit;
using Microsoft.AspNetCore.Components;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components.Pages;
using TestContext = Xunit.TestContext;

namespace ServiceBusDemo.Tests;

public class HomeBurstTests
{
    [Theory]
    [InlineData(false, "/sendMessage", "CompetingConsumers", 2)]
    [InlineData(true, "/broadcastMessage", "PubSub", 4)]
    public async Task TracksProgressAndEarlyAndLateReports(bool pubSub, string route, string pattern, int expected)
    {
        await using var demo = new Demo();
        if (pubSub)
        {
            var radio = demo.Home.Find("#pattern-pubsub");
            radio.Change(((IHtmlInputElement)radio).Value);
        }
        var sending = demo.Start();
        var first = await demo.Handler.NextAsync();
        Assert.Equal((route, "Order 42"), (first.Route, first.Message));
        Assert.Single(demo.Home.FindAll("button[type=submit][disabled]"));
        Assert.Single(demo.Home.FindAll("label[for=burst-count]"));
        Assert.Single(demo.Home.FindAll("label[for=burst-progress]"));
        Assert.Single(demo.Home.FindAll("label[for=burst-ConsumerOne]"));
        demo.Report("first", "ConsumerOne", pattern);
        demo.Home.WaitForAssertion(() => Assert.Equal("0", demo.Progress));
        first.Accept("first");
        var second = await demo.Handler.NextAsync();
        Assert.Equal((route, "Order 42"), (second.Route, second.Message));
        demo.Home.WaitForAssertion(() =>
        {
            Assert.Equal("1", demo.Progress);
            Assert.Equal("2", demo.Home.Find("#burst-progress").GetAttribute("max"));
            Assert.Equal("1", demo.Consumer("One"));
        });
        second.Accept("second");
        await sending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        demo.Report("second", "ConsumerTwo", pattern);
        demo.Report("second", "ConsumerTwo", pattern);
        demo.Report("unrelated", "ConsumerTwo", pattern);
        if (pubSub)
        {
            demo.Report("first", "ConsumerTwo", pattern);
            demo.Report("second", "ConsumerOne", pattern);
        }
        demo.Home.WaitForAssertion(() =>
        {
            Assert.Equal($"Reported deliveries: {expected} / {expected} expected", demo.Total);
            Assert.Equal(pubSub ? "2" : "1", demo.Consumer("One"));
            Assert.Equal(pubSub ? "2" : "1", demo.Consumer("Two"));
        });
        demo.Home.Find(".deliveries button").Click();
        Assert.Empty(demo.Home.FindAll(".delivery-card"));
        Assert.Equal($"Reported deliveries: {expected} / {expected} expected", demo.Total);
        Assert.Equal(2, demo.Home.FindAll("tbody tr").Count);
    }

    [Theory]
    [InlineData("0", null, "count")]
    [InlineData("501", null, "count")]
    [InlineData("1.5", null, "count")]
    [InlineData("", null, "count")]
    [InlineData("500", "1001", "rate")]
    [InlineData("1", "0", "rate")]
    [InlineData("1", "invalid", "rate")]
    [InlineData("1", "", "rate")]
    public async Task InvalidBurstInputDoesNotSend(string count, string? rate, string field)
    {
        await using var demo = new Demo();
        if (rate is not null)
        {
            demo.Pace(rate);
        }
        await demo.Start(count);
        Assert.Empty(demo.Home.FindAll("#burst-progress"));
        Assert.Equal("true", demo.Home.Find($"#burst-{field}").GetAttribute("aria-invalid"));
        Assert.Contains("whole number", demo.Home.Find($"#burst-{field}-error").TextContent);
        Assert.Equal(0, demo.Handler.RequestCount);
    }

    [Theory]
    [InlineData("count")]
    [InlineData("rate")]
    public async Task ImplicitSubmissionFromBurstFieldValidatesAndStartsOnlyABurst(string field)
    {
        await using var demo = new Demo();
        demo.Pace("1000");
        demo.Home.Find("#burst-count").Input("2");
        var selector = $"#burst-{field}";
        var input = (IHtmlInputElement)demo.Home.Find(selector);
        var form = Assert.IsAssignableFrom<IHtmlFormElement>(input.Form);
        Assert.Equal("burst-form", form.Id);
        Assert.Equal("send-burst", Assert.Single(form.QuerySelectorAll("button[type=submit]")).Id);
        Assert.NotEqual(form.Id, ((IHtmlTextAreaElement)demo.Home.Find("#message")).Form!.Id);

        // bUnit does not execute the browser's Enter default action. Exercise the submit
        // event on the input's actual form, which the browser's default button dispatches.
        var formSelector = $"#{form.Id}";
        demo.Home.Find(selector).Input("invalid");
        await demo.Home.Find(formSelector).SubmitAsync();
        Assert.Equal(0, demo.Handler.RequestCount);
        Assert.Equal("true", demo.Home.Find(selector).GetAttribute("aria-invalid"));

        demo.Home.Find(selector).Input("2");
        var sending = demo.Home.Find(formSelector).SubmitAsync();
        (await demo.Handler.NextAsync()).Accept("first");
        (await demo.Handler.NextAsync()).Accept("second");
        await sending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal("2", demo.Progress);
        Assert.Equal("Burst complete.", demo.Status);
        Assert.Empty(demo.Home.FindAll(".alert-success"));
    }

    [Fact]
    public async Task SharedMessageValidationAppliesToBurstAndBurstErrorsDoNotBlockSingleSend()
    {
        await using var demo = new Demo();
        demo.Home.Find("#message").Change("   ");
        await demo.Start();
        Assert.Contains("Please enter a message", demo.Home.Markup);
        Assert.Equal(0, demo.Handler.RequestCount);
        demo.Home.Find("#message").Change("Order 42");
        await demo.Start("bad count");
        var sending = demo.Home.Find("form").SubmitAsync();
        var request = await demo.Handler.NextAsync();
        request.Accept("single");
        await sending;
        Assert.Single(demo.Home.FindAll("tbody tr"));
        Assert.Empty(demo.Home.FindAll("#burst-progress"));
    }

    [Fact]
    public async Task FixedRateSpacesStartsAndCancelInterruptsTheNextDelay()
    {
        await using var demo = new Demo();
        demo.Pace("2");
        var sending = demo.Start("3");
        var first = await demo.Handler.NextAsync();
        first.Accept("first");
        var second = await demo.Handler.NextAsync();
        Assert.True(Stopwatch.GetElapsedTime(first.Started, second.Started) >= TimeSpan.FromSeconds(0.5));
        await demo.Home.InvokeAsync(() =>
        {
            second.Accept("second");
        });
        demo.Home.WaitForAssertion(() => Assert.Equal("2", demo.Progress));
        await demo.Home.Find("#cancel-burst").ClickAsync(new());
        await sending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal("Burst cancelled.", demo.Status);
        Assert.Equal(2, demo.Handler.RequestCount);
        Assert.Equal("2", demo.Progress);
    }

    [Fact]
    public async Task FixedRateStillSpacesStartsWhenFirstRequestSetupIsDelayed()
    {
        await using var demo = new Demo(new DelayFirstRequest(TimeSpan.FromMilliseconds(350)));
        demo.Pace("4");
        var sending = demo.Start();
        var first = await demo.Handler.NextAsync();
        var acknowledgedAt = Stopwatch.GetTimestamp();
        first.Accept("first");
        var second = await demo.Handler.NextAsync();
        Assert.True(Stopwatch.GetElapsedTime(acknowledgedAt, second.Started) >= TimeSpan.FromMilliseconds(250));
        second.Accept("second");
        await sending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal("Burst complete.", demo.Status);
    }

    [Fact]
    public async Task CancelLeavesInFlightRequestRunningAndCountsItsResponse()
    {
        await using var demo = new Demo();
        var sending = demo.Start();
        var first = await demo.Handler.NextAsync();
        demo.Home.WaitForAssertion(() => Assert.Single(demo.Context.JSInterop.Invocations["Blazor._internal.domWrapper.focus"]));
        await demo.Home.Find("#cancel-burst").ClickAsync(new());
        Assert.Single(demo.Home.FindAll("#cancel-burst[aria-disabled=true]:not([disabled])"));
        Assert.Contains("Cancellation pending", demo.Status);
        Assert.False(first.Token.IsCancellationRequested);
        Assert.False(sending.IsCompleted);
        demo.Report("first", "ConsumerOne");
        first.Accept("first");
        await sending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Single(demo.Home.FindAll("#send-burst[type=submit]:not([disabled]):not([aria-disabled])"));
        Assert.Equal("Burst cancelled.", demo.Status);
        Assert.Equal(1, demo.Handler.RequestCount);
        Assert.Equal("1", demo.Progress);
        // At-least-once delivery to the other competing consumer can exceed expected.
        demo.Report("first", "ConsumerTwo");
        demo.Home.WaitForAssertion(() =>
        {
            Assert.Equal("Reported deliveries: 2 / 1 expected", demo.Total);
            Assert.All(demo.Home.FindAll(".burst-results progress"), bar =>
                Assert.InRange(int.Parse(bar.GetAttribute("value")!), 0, int.Parse(bar.GetAttribute("max")!)));
        });
    }

    [Fact]
    public async Task DecliningNavigationKeepsBurstRunningAndCancellingDisarmsThePrompt()
    {
        await using var demo = new Demo();
        var navigation = demo.Context.Services.GetRequiredService<NavigationManager>();
        var originalUri = navigation.Uri;
        demo.Context.JSInterop.Setup<bool>("confirm", _ => true).SetResult(false);
        var warning = demo.Home.Find("#burst-navigation-warning[role=status][aria-live=polite][aria-atomic=true]");
        Assert.Empty(warning.TextContent);
        Assert.Empty(demo.Context.JSInterop.Invocations["Blazor._internal.NavigationLock.enableNavigationPrompt"]);

        var sending = demo.Start();
        var first = await demo.Handler.NextAsync();
        demo.Home.WaitForAssertion(() => Assert.Single(
            demo.Context.JSInterop.Invocations["Blazor._internal.NavigationLock.enableNavigationPrompt"]));
        Assert.Contains("may still accept an in-flight message", warning.TextContent);
        await demo.Home.InvokeAsync(() => navigation.NavigateTo("/Error"));
        var confirmation = Assert.Single(demo.Context.JSInterop.Invocations["confirm"]);
        Assert.Equal(originalUri, navigation.Uri);
        Assert.Contains(warning.TextContent, Assert.IsType<string>(confirmation.Arguments[0]));
        Assert.False(first.Token.IsCancellationRequested);

        await demo.Home.Find("#cancel-burst").ClickAsync(new());
        Assert.Contains("may still accept an in-flight message", warning.TextContent);
        Assert.Empty(demo.Context.JSInterop.Invocations["Blazor._internal.NavigationLock.disableNavigationPrompt"]);
        first.Accept("first");
        await sending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal("Burst cancelled.", demo.Status);
        Assert.Equal("1", demo.Progress);
        Assert.Empty(warning.TextContent);
        demo.Home.WaitForAssertion(() => Assert.Single(
            demo.Context.JSInterop.Invocations["Blazor._internal.NavigationLock.disableNavigationPrompt"]));
        await demo.Home.InvokeAsync(() => navigation.NavigateTo("/Error"));
        Assert.EndsWith("/Error", navigation.Uri);
        Assert.Single(demo.Context.JSInterop.Invocations["confirm"]);
    }

    [Fact]
    public async Task CoalescesSharedDeliveryAnnouncementsDuringBurstAndAnnouncesTheFinalGroup()
    {
        const string deliveryStatus = "#delivery-status[role=status][aria-live=polite][aria-atomic=true] span";
        await using var demo = new Demo();
        var sending = demo.Start();
        var request = await demo.Handler.NextAsync();
        await demo.Home.InvokeAsync(() =>
        {
            demo.Report("other-tab-1", "ConsumerOne");
            demo.Report("other-tab-2", "ConsumerTwo");
            Assert.Equal(2, demo.Home.FindAll(".delivery-card").Count);
            Assert.Empty(demo.Home.Find(deliveryStatus).TextContent);
        });
        await demo.Home.WaitForAssertionAsync(() =>
        {
            Assert.Equal("2 new delivery reports.", demo.Home.Find(deliveryStatus).TextContent);
            Assert.False(sending.IsCompleted);
            Assert.Equal("Reported deliveries: 0 / 0 expected", demo.Total);
        }, TimeSpan.FromSeconds(5));

        request.Accept("first");
        var last = await demo.Handler.NextAsync();
        await demo.Home.InvokeAsync(() =>
        {
            demo.Report("first", "ConsumerOne");
            demo.Report("second", "ConsumerTwo");
            demo.Report("other-tab-3", "ConsumerTwo");
            Assert.Equal("2 new delivery reports.", demo.Home.Find(deliveryStatus).TextContent);
            last.Accept("second");
        });
        await sending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await demo.Home.WaitForAssertionAsync(() =>
        {
            Assert.Equal("Burst complete.", demo.Status);
            Assert.Equal("Burst: 2 of 2 sends acknowledged. Reported deliveries: Consumer One 1, Consumer Two 1.",
                demo.Home.Find("#burst-counts-status").TextContent);
            Assert.Equal("3 new delivery reports.", demo.Home.Find(deliveryStatus).TextContent);
        }, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task ConfirmingNavigationAllowsLeavingDuringBurst()
    {
        await using var demo = new Demo();
        var navigation = demo.Context.Services.GetRequiredService<NavigationManager>();
        demo.Context.JSInterop.Setup<bool>("confirm", _ => true).SetResult(true);
        var sending = demo.Start();
        await demo.Handler.NextAsync();
        await demo.Home.InvokeAsync(() => navigation.NavigateTo("/Error"));
        demo.Home.WaitForAssertion(() => Assert.EndsWith("/Error", navigation.Uri));
        Assert.Single(demo.Context.JSInterop.Invocations["confirm"]);
        await demo.Context.DisposeComponentsAsync();
        await sending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task FailureKeepsAcknowledgementsAndNewBurstReplacesOnlyStatistics()
    {
        await using var demo = new Demo();
        var sending = demo.Start("3");
        (await demo.Handler.NextAsync()).Accept("first");
        var failed = await demo.Handler.NextAsync();
        demo.Report("failed", "ConsumerOne");
        failed.Response.SetResult(new(HttpStatusCode.ServiceUnavailable));
        await sending;
        demo.Report("first", "ConsumerOne");
        demo.Home.WaitForAssertion(() => Assert.Equal("Reported deliveries: 1 / 1 expected", demo.Total));
        Assert.Equal(2, demo.Handler.RequestCount);
        Assert.Equal("1", demo.Progress);
        Assert.Contains("may have accepted", demo.Status);
        var previous = Assert.Single(BurstDeliveryTrackerTests.Subscribers(demo.Feed)).Target;

        var next = demo.Start("1");
        var request = await demo.Handler.NextAsync();
        Assert.NotSame(previous, Assert.Single(BurstDeliveryTrackerTests.Subscribers(demo.Feed)).Target);
        request.Accept("new");
        await next;
        demo.Report("first", "ConsumerTwo");
        demo.Home.WaitForAssertion(() => Assert.Equal("Reported deliveries: 0 / 1 expected", demo.Total));
        Assert.Equal(2, demo.Home.FindAll("tbody tr").Count);
        Assert.Equal(3, demo.Home.FindAll(".delivery-card").Count);
    }

    [Fact]
    public async Task OverflowIsVisibleAndComponentDisposalCancelsTheRequestAndDetachesTracker()
    {
        await using var demo = new Demo();
        var sending = demo.Start();
        var first = await demo.Handler.NextAsync();
        // Keep the assertion from waiting behind hundreds of separately queued renderer work items.
        await demo.Home.InvokeAsync(() =>
        {
            for (var i = 0; i <= BurstDeliveryTracker.EarlyIdCapacity; i++)
            {
                demo.Report($"other-{i}", "ConsumerOne");
            }
        });
        await demo.Home.WaitForAssertionAsync(() => Assert.Contains("early-report buffer filled up", demo.Home.Markup));
        await demo.Context.DisposeComponentsAsync();
        Assert.True(first.Token.IsCancellationRequested);
        Assert.Empty(BurstDeliveryTrackerTests.Subscribers(demo.Feed));
        await sending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(1, demo.Handler.RequestCount);
    }

    private sealed class Demo : IAsyncDisposable
    {
        private readonly HttpClient _client;
        public BunitContext Context { get; } = new();
        public ControlledHandler Handler { get; } = new();
        public DeliveryFeed Feed { get; } = new();
        public IRenderedComponent<Home> Home { get; }
        public string? Progress => Home.Find("#burst-progress").GetAttribute("value");
        public string Status => Home.Find("#burst-status").TextContent;
        public string Total => Home.Find("#burst-total").TextContent;

        public Demo(DelegatingHandler? requestSetup = null)
        {
            if (requestSetup is not null)
            {
                requestSetup.InnerHandler = Handler;
            }
            _client = new HttpClient(requestSetup is null ? Handler : requestSetup) { BaseAddress = new Uri("http://producer.test") };
            Context.JSInterop.SetupVoid("Blazor._internal.domWrapper.focus", _ => true).SetVoidResult();
            Context.JSInterop.SetupVoid("Blazor._internal.NavigationLock.enableNavigationPrompt", _ => true).SetVoidResult();
            Context.JSInterop.SetupVoid("Blazor._internal.NavigationLock.disableNavigationPrompt", _ => true).SetVoidResult();
            Context.Services.AddSingleton(new ProducerApiClient(_client));
            Context.Services.AddSingleton(Feed);
            Home = Context.Render<Home>();
            Assert.Equal("10", Home.Find("#burst-count").GetAttribute("value"));
            Home.Find("#message").Change(" Order 42 \t");
        }

        public string? Consumer(string suffix) => Home.Find($"#burst-Consumer{suffix}").GetAttribute("value");

        public Task Start(string count = "2")
        {
            Home.Find("#burst-count").Input(count);
            return Home.Find("#burst-form").SubmitAsync();
        }

        public void Pace(string rate)
        {
            Home.Find("#burst-paced").Change(true);
            Home.Find("#burst-rate").Input(rate);
        }

        public void Report(string id, string consumer, string pattern = "CompetingConsumers") =>
            Feed.Add(new DeliveryEvent(id, pattern, consumer, "topic", "subscription", "Order 42", 1, DateTimeOffset.UnixEpoch));

        public async ValueTask DisposeAsync()
        {
            await Context.DisposeAsync();
            _client.Dispose();
        }
    }

    private sealed class DelayFirstRequest(TimeSpan delay) : DelegatingHandler
    {
        private int _calls;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _calls) == 1)
            {
                await Task.Delay(delay, cancellationToken);
            }
            return await base.SendAsync(request, cancellationToken);
        }
    }

    private sealed record Request(string Route, string Message, long Started, CancellationToken Token)
    {
        public TaskCompletionSource<HttpResponseMessage> Response { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Accept(string id) => Response.SetResult(new(HttpStatusCode.Accepted) { Content = JsonContent.Create(new SendMessageResult(id, "topic")) });
    }

    private sealed class ControlledHandler : HttpMessageHandler
    {
        private readonly System.Threading.Channels.Channel<Request> _requests = System.Threading.Channels.Channel.CreateUnbounded<Request>();
        private int _count;
        public int RequestCount => Volatile.Read(ref _count);

        public Task<Request> NextAsync() => _requests.Reader.ReadAsync(TestContext.Current.CancellationToken).AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var started = Stopwatch.GetTimestamp();
            var body = await request.Content!.ReadFromJsonAsync<SendMessageRequest>(cancellationToken);
            var pending = new Request(request.RequestUri!.AbsolutePath, body!.Message, started, cancellationToken);
            Interlocked.Increment(ref _count);
            await _requests.Writer.WriteAsync(pending, cancellationToken);
            return await pending.Response.Task.WaitAsync(cancellationToken);
        }
    }
}
