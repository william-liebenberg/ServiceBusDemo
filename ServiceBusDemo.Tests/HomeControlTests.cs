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
        Assert.True(home.Find("#ConsumerOne .consumer-pause").HasAttribute("disabled"));
        initial.SetResult();
        home.WaitForAssertion(() => Assert.Equal("Running", State(home, "ConsumerOne")), Deadline);
        home.WaitForAssertion(() => Assert.Equal("Running", State(home, "ConsumerTwo")), Deadline);
        var changing = home.Find("#ConsumerTwo .consumer-pause").ClickAsync(new());
        home.WaitForAssertion(() =>
        {
            Assert.Equal("Updating…", State(home, "ConsumerTwo"));
            Assert.True(home.Find("#ConsumerTwo .consumer-pause").HasAttribute("disabled"));
            Assert.True(home.Find("#ConsumerTwo .consumer-resume").HasAttribute("disabled"));
            Assert.False(home.Find("#ConsumerOne .consumer-pause").HasAttribute("disabled"));
        }, Deadline);
        pause.SetResult();
        await changing.WaitAsync(Deadline, token);
        home.WaitForAssertion(() =>
        {
            Assert.Equal("Mixed", State(home, "ConsumerTwo"));
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
        }, Deadline);
        Volatile.Write(ref twoState, (int)ConsumerState.Running); // Process restart resets pause.
        home.WaitForAssertion(() =>
        {
            Assert.Equal("Running", State(home, "ConsumerTwo"));
            Assert.Empty(home.Find("#ConsumerTwo .consumer-error").TextContent);
        }, Deadline);
        Assert.Equal(2, twoPosts);
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

    private static BunitContext CreateContext(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send)
    {
        var context = new BunitContext();
        context.Services.AddSingleton<ProducerApiClient>(_ => new(new HttpClient()));
        context.Services.AddSingleton<DeliveryFeed>();
        context.Services.AddControlTestClients(send);
        return context;
    }
}
