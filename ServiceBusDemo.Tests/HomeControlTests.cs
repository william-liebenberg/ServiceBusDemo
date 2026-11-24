using System.Net.Http.Json;
using Bunit;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components.Pages;
using XunitTestContext = Xunit.TestContext;

namespace ServiceBusDemo.Tests;

public class HomeControlTests
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task LaneActionsStayIndependentAndPollingReflectsRestart()
    {
        var token = XunitTestContext.Current.CancellationToken;
        var initial = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pause = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resumed = 0;
        var twoPosts = 0;
        var twoState = (int)ConsumerState.Running;
        await using var context = CreateContext(async (request, ct) =>
        {
            if (request.RequestUri!.Host == "consumer-one.test")
            {
                Assert.Equal(HttpMethod.Get, request.Method);
                await initial.Task.WaitAsync(ct);
                return ControlTestClients.Reply("ConsumerOne", ConsumerState.Running);
            }
            if (request.Method == HttpMethod.Post)
            {
                Assert.Equal("1", Assert.Single(request.Headers.GetValues(ConsumerControlProtocol.HeaderName)));
                Interlocked.Increment(ref twoPosts);
                if (request.RequestUri.AbsolutePath == "/control/pause")
                {
                    await pause.Task.WaitAsync(ct);
                    Volatile.Write(ref twoState, (int)ConsumerState.Mixed);
                    return ControlTestClients.Reply("ConsumerTwo", ConsumerState.Mixed, HttpStatusCode.ServiceUnavailable);
                }
                Assert.Equal("/control/resume", request.RequestUri.AbsolutePath);
                Interlocked.Increment(ref resumed);
                Volatile.Write(ref twoState, (int)ConsumerState.Running);
            }
            if ((ConsumerState)Volatile.Read(ref twoState) == ConsumerState.Unavailable)
            {
                return new HttpResponseMessage(HttpStatusCode.BadGateway);
            }
            return ControlTestClients.Reply("ConsumerTwo", (ConsumerState)Volatile.Read(ref twoState));
        });
        var home = context.Render<Home>();
        Assert.Equal("Checking status…", State(home, "ConsumerOne"));
        AssertFocusableAndDisabled(home, "#ConsumerOne .consumer-pause");
        initial.SetResult();
        home.WaitForAssertion(() => Assert.Equal("Running", State(home, "ConsumerOne")), Deadline);
        home.WaitForAssertion(() => Assert.Equal("Running", State(home, "ConsumerTwo")), Deadline);
        var changing = home.Find("#ConsumerTwo .consumer-pause").ClickAsync(new());
        home.WaitForAssertion(() =>
        {
            Assert.Equal("Updating…", State(home, "ConsumerTwo"));
            AssertFocusableAndDisabled(home, "#ConsumerTwo .consumer-pause");
            AssertFocusableAndDisabled(home, "#ConsumerTwo .consumer-resume");
            Assert.False(home.Find("#ConsumerOne .consumer-pause").HasAttribute("aria-disabled"));
        }, Deadline);
        pause.SetResult();
        await changing.WaitAsync(Deadline, token);
        home.WaitForAssertion(() =>
        {
            Assert.Equal("Mixed", State(home, "ConsumerTwo"));
            Assert.StartsWith("Consumer Two:", home.Find("#ConsumerTwo .consumer-error").TextContent.Trim());
            Assert.Equal("true", home.Find("#ConsumerTwo .consumer-error").GetAttribute("aria-atomic"));
            Assert.Contains("try again", home.Find("#ConsumerTwo .consumer-error").TextContent);
        }, Deadline);
        await home.Find("#ConsumerTwo .consumer-resume").ClickAsync(new()).WaitAsync(Deadline, token);
        Assert.Equal(1, resumed);
        Assert.Equal("Running", State(home, "ConsumerTwo"));
        Volatile.Write(ref twoState, (int)ConsumerState.Paused); // Another tab pauses the consumer.
        home.WaitForAssertion(() => Assert.Equal("Paused", State(home, "ConsumerTwo")), Deadline);
        Volatile.Write(ref twoState, (int)ConsumerState.Unavailable);
        home.WaitForAssertion(() =>
        {
            Assert.Equal("Unavailable", State(home, "ConsumerTwo"));
            Assert.Contains("retry automatically", home.Find("#ConsumerTwo .consumer-error").TextContent);
            Assert.StartsWith("Consumer Two:", home.Find("#ConsumerTwo .consumer-error").TextContent.Trim());
        }, Deadline);
        Volatile.Write(ref twoState, (int)ConsumerState.Running); // Process restart resets pause.
        home.WaitForAssertion(() =>
        {
            Assert.Equal("Running", State(home, "ConsumerTwo"));
            Assert.Empty(home.Find("#ConsumerTwo .consumer-error").TextContent);
        }, Deadline);
        Assert.Equal(2, twoPosts);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task KeyboardActivationStaysFocusableAndPollingConfirmsLateTransition(bool pause)
    {
        var token = XunitTestContext.Current.CancellationToken;
        var initial = pause ? ConsumerState.Running : ConsumerState.Paused;
        var target = pause ? ConsumerState.Paused : ConsumerState.Running;
        var state = (int)initial;
        var action = pause ? "pause" : "resume";
        var selector = $"#ConsumerOne .consumer-{action}";
        var otherSelector = $"#ConsumerOne .consumer-{(pause ? "resume" : "pause")}";
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var posts = 0;
        await using var context = CreateContext(async (request, ct) =>
        {
            if (request.RequestUri!.Host == "consumer-two.test")
            {
                Assert.Equal(HttpMethod.Get, request.Method);
                return ControlTestClients.Reply("ConsumerTwo", ConsumerState.Running);
            }
            if (request.Method == HttpMethod.Post)
            {
                Assert.Equal($"/control/{action}", request.RequestUri.AbsolutePath);
                Assert.Equal(1, Interlocked.Increment(ref posts));
                await release.Task.WaitAsync(ct);
                throw new HttpRequestException("Response lost before transition was confirmed.");
            }
            return ControlTestClients.Reply("ConsumerOne", (ConsumerState)Volatile.Read(ref state));
        });
        var home = context.Render<Home>();
        home.WaitForAssertion(() => Assert.Equal(initial.ToString(), State(home, "ConsumerOne")), Deadline);

        // bUnit does not synthesize native keyboard clicks; dispatch the click a browser emits for Enter/Space.
        Assert.Equal("button", home.Find(selector).LocalName);
        Assert.False(home.Find(selector).HasAttribute("tabindex"));
        var changing = home.Find(selector).ClickAsync(new() { Detail = 0 });
        home.WaitForAssertion(() => Assert.Equal("Updating…", State(home, "ConsumerOne")), Deadline);
        AssertFocusableAndDisabled(home, selector);
        AssertFocusableAndDisabled(home, otherSelector);
        await home.Find(selector).ClickAsync(new() { Detail = 0 }).WaitAsync(Deadline, token);
        await home.Find(otherSelector).ClickAsync(new() { Detail = 0 }).WaitAsync(Deadline, token);
        release.SetResult();
        await changing.WaitAsync(Deadline, token);
        Assert.Contains($"Consumer One: Could not confirm {action}", home.Find("#ConsumerOne .consumer-error").TextContent);
        home.WaitForAssertion(() => Assert.Equal(initial.ToString(), State(home, "ConsumerOne")), Deadline);
        Assert.Contains("try again", home.Find("#ConsumerOne .consumer-error").TextContent);
        Volatile.Write(ref state, (int)target);
        home.WaitForAssertion(() =>
        {
            Assert.Equal(target.ToString(), State(home, "ConsumerOne"));
            Assert.True(string.IsNullOrWhiteSpace(home.Find("#ConsumerOne .consumer-error").TextContent));
            AssertFocusableAndDisabled(home, selector);
        }, Deadline);
        Assert.Equal(1, posts);
    }

    [Fact]
    public async Task DisposingPageCancelsBothPendingStatusRequests()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var canceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var starts = 0;
        var cancels = 0;
        await using var context = CreateContext(async (_, ct) =>
        {
            if (Interlocked.Increment(ref starts) == 2) started.TrySetResult();
            try { await Task.Delay(Timeout.InfiniteTimeSpan, ct); }
            finally { if (Interlocked.Increment(ref cancels) == 2) canceled.TrySetResult(); }
            throw new InvalidOperationException("Expected cancellation.");
        });
        context.Render<Home>();
        var token = XunitTestContext.Current.CancellationToken;
        await started.Task.WaitAsync(Deadline, token);
        await context.DisposeComponentsAsync();
        await canceled.Task.WaitAsync(Deadline, token);
    }

    [Fact]
    public async Task TypedClientRejectsWrongConsumerAndAnUnconfirmedSuccess()
    {
        var token = XunitTestContext.Current.CancellationToken;
        using var http = new HttpClient(new ControlTestClients.Handler((request, _) =>
        {
            if (request.Method == HttpMethod.Get)
            {
                Assert.Equal("/control/status", request.RequestUri!.AbsolutePath);
                Assert.False(request.Headers.Contains(ConsumerControlProtocol.HeaderName));
                return Task.FromResult(ControlTestClients.Reply("ConsumerTwo", ConsumerState.Running));
            }
            Assert.Equal("/control/pause", request.RequestUri!.AbsolutePath);
            Assert.Equal("1", Assert.Single(request.Headers.GetValues(ConsumerControlProtocol.HeaderName)));
            return Task.FromResult(ControlTestClients.Reply("ConsumerOne", ConsumerState.Running));
        })) { BaseAddress = new Uri("http://consumer-one.test") };
        var client = new ConsumerOneControlClient(http);
        await Assert.ThrowsAsync<InvalidOperationException>(() => client.GetStatusAsync(token));
        var failure = await Assert.ThrowsAsync<ConsumerTransitionException>(() => client.PauseAsync(token));
        Assert.Equal(ConsumerState.Running, failure.Status.State);
    }

    private static string State(IRenderedComponent<Home> home, string consumer) => home.Find($"#{consumer} .consumer-state").TextContent;
    private static void AssertFocusableAndDisabled(IRenderedComponent<Home> home, string selector)
    {
        var button = home.Find(selector);
        Assert.Equal("true", button.GetAttribute("aria-disabled"));
        Assert.False(button.HasAttribute("disabled"));
        Assert.False(button.HasAttribute("tabindex"));
    }


    private static BunitContext CreateContext(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send)
    {
        var context = new BunitContext();
        context.Services.AddSingleton<ProducerApiClient>(_ => new(new HttpClient()));
        context.Services.AddSingleton<DeliveryFeed>();
        context.Services.AddControlTestClients(send);
        return context;
    }
}
