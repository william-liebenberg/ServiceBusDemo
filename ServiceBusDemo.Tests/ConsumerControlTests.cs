using System.Net.Http.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Hosting;
using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Tests;

public class ConsumerControlTests
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(true, "ConsumerOne", "consumer-one")]
    [InlineData(false, "ConsumerTwo", "consumer-two")]
    public async Task EndpointsEnforceHeaderAndReportConfiguredProcessors(bool first, string name, string broadcast)
    {
        var bus = new FakeBus(first ? 4 : 3);
        if (first)
        {
            await VerifyEndpointsAsync<ConsumerOne.MessageConsumer>(bus, name, broadcast);
        }
        else
        {
            await VerifyEndpointsAsync<ConsumerTwo.MessageConsumer>(bus, name, broadcast);
        }
    }

    private static async Task VerifyEndpointsAsync<TConsumer>(FakeBus bus, string name, string broadcast)
        where TConsumer : ControlledMessageConsumer
    {
        await using var factory = new WebApplicationFactory<TConsumer>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("ConnectionStrings:messaging", "Endpoint=sb://localhost/;SharedAccessKeyName=test;SharedAccessKey=test");
            builder.UseSetting("ServiceBus:TopicName", "configured-topic");
            builder.UseSetting("ServiceBus:SubscriptionName", "configured-subscription");
            builder.UseSetting("ServiceBus:RoutedTopicName", "configured-routed");
            builder.UseSetting("ServiceBus:HighPrioritySubscriptionName", "configured-high");
            builder.UseSetting("ServiceBus:AllRoutedSubscriptionName", "configured-all");
            builder.UseSetting("ServiceBus:EuOrdersSubscriptionName", "configured-eu");
            builder.ConfigureTestServices(services => services.AddSingleton<ServiceBusClient>(bus));
        });
        using var http = factory.CreateClient(new() { BaseAddress = new Uri("https://localhost"), AllowAutoRedirect = false });
        var consumer = factory.Services.GetRequiredService<TConsumer>();
        Assert.Same(consumer, Assert.Single(factory.Services.GetServices<IHostedService>().OfType<TConsumer>()));
        await consumer.ExecuteTask!.WaitAsync(Deadline, Token);
        var status = await http.GetFromJsonAsync<ConsumerStatus>("/control/status", Token);
        Assert.Equal(name, status!.Consumer);
        Assert.Equal(ConsumerState.Running, status.State);
        string[] routed = name == "ConsumerOne" ? ["configured-high", "configured-all"] : ["configured-eu"];
        SubscriptionStatus[] expectedSubscriptions =
        [
            new("CompetingConsumers", "configured-topic", "configured-subscription", ConsumerState.Running),
            new("PubSub", "broadcast-events", broadcast, ConsumerState.Running),
            .. routed.Select(subscription => new SubscriptionStatus("TopicFilters", "configured-routed", subscription, ConsumerState.Running))
        ];
        Assert.Equal(expectedSubscriptions, status.Subscriptions);
        Assert.Equal(expectedSubscriptions.Select(s => (s.Topic, s.Subscription)), bus.Entities);
        foreach (var action in new[] { "pause", "resume" })
        {
            foreach (var invalidHeader in new[] { null, "wrong", "1, 1" })
            {
                using var rejected = await PostAsync(http, action, invalidHeader);
                Assert.Equal(HttpStatusCode.BadRequest, rejected.StatusCode);
            }
        }
        using var unexpectedHost = await PostAsync(http, "pause", ConsumerControlProtocol.HeaderValue, "unexpected.example:5196");
        Assert.Equal(HttpStatusCode.BadRequest, unexpectedHost.StatusCode);
        Assert.All(bus.Processors, processor => Assert.Equal(0, processor.Stops));
        Assert.Equal(ConsumerState.Running, (await http.GetFromJsonAsync<ConsumerStatus>("/control/status", Token))!.State);

        bus.Processors[1].FailStop = true;
        using var failed = await PostAsync(http, "pause", ConsumerControlProtocol.HeaderValue);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, failed.StatusCode);
        var mixed = await failed.Content.ReadFromJsonAsync<ConsumerStatus>(Token);
        Assert.Equal(ConsumerState.Mixed, mixed!.State);
        Assert.Equal(expectedSubscriptions.Select((_, i) => i == 1 ? ConsumerState.Running : ConsumerState.Paused),
            mixed.Subscriptions.Select(s => s.State));
        bus.Processors[1].FailStop = false;

        foreach (var (action, expected) in new[] { ("pause", ConsumerState.Paused), ("resume", ConsumerState.Running) })
        {
            for (var i = 0; i < 2; i++)
            {
                using var response = await PostAsync(http, action, ConsumerControlProtocol.HeaderValue);
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                var result = await response.Content.ReadFromJsonAsync<ConsumerStatus>(Token);
                Assert.Equal(expected, result!.State);
                Assert.All(result.Subscriptions, subscription => Assert.Equal(expected, subscription.State));
            }
        }
        Assert.Equal(1, bus.Processors[0].Stops);
        Assert.Equal(2, bus.Processors[1].Stops);
        Assert.All(bus.Processors.Skip(2), p => Assert.Equal(1, p.Stops));
        Assert.All(bus.Processors, p => { Assert.Equal(2, p.Starts); Assert.Equal(0, p.Closes); });
        await consumer.StopAsync(Token);
        using var unavailable = await PostAsync(http, "pause", ConsumerControlProtocol.HeaderValue);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, unavailable.StatusCode);
        Assert.Equal(ConsumerState.Unavailable, (await unavailable.Content.ReadFromJsonAsync<ConsumerStatus>(Token))!.State);
        Assert.All(bus.Processors, p => Assert.Equal(1, p.Closes));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task InitializationTransitionsAndShutdownDoNotOverlap(bool first)
    {
        var bus = new FakeBus(first ? 4 : 3);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDeliveryReporting();
        using var host = builder.Build();
        var queue = host.Services.GetRequiredService<DeliveryReportQueue>();
        using ControlledMessageConsumer consumer = first
            ? new ConsumerOne.MessageConsumer(bus, builder.Configuration, host.Services.GetRequiredService<Microsoft.Extensions.Logging.ILogger<ConsumerOne.MessageConsumer>>(), queue)
            : new ConsumerTwo.MessageConsumer(bus, builder.Configuration, host.Services.GetRequiredService<Microsoft.Extensions.Logging.ILogger<ConsumerTwo.MessageConsumer>>(), queue);
        var startup = NewSignal();
        bus.Processors[0].BeforeStart = startup.Task;
        await consumer.StartAsync(Token);
        Assert.Equal(ConsumerState.Initializing, consumer.GetStatus().State);
        Assert.Equal(first ? 4 : 3, consumer.GetStatus().Subscriptions.Count);
        Assert.False((await consumer.PauseAsync(Token)).Succeeded);
        startup.SetResult();
        await consumer.ExecuteTask!.WaitAsync(Deadline, Token);

        var release = NewSignal();
        bus.Processors[0].BeforeStop = release.Task;
        var pause = consumer.PauseAsync(Token);
        var resume = consumer.ResumeAsync(Token);
        try
        {
            Assert.Equal(ConsumerState.Transitioning, consumer.GetStatus().State);
            Assert.False(pause.IsCompleted);
            Assert.False(resume.IsCompleted);
        }
        finally { release.TrySetResult(); }
        Assert.Equal(ConsumerState.Paused, (await pause.WaitAsync(Deadline, Token)).Status.State);
        Assert.Equal(ConsumerState.Running, (await resume.WaitAsync(Deadline, Token)).Status.State);

        release = NewSignal();
        bus.Processors[0].BeforeStop = release.Task;
        pause = consumer.PauseAsync(Token);
        var shutdown = consumer.StopAsync(Token);
        try
        {
            Assert.False(shutdown.IsCompleted);
            Assert.All(bus.Processors, p => Assert.Equal(0, p.Closes));
            Assert.Equal(ConsumerState.Unavailable, consumer.GetStatus().State);
            Assert.False((await consumer.ResumeAsync(Token)).Succeeded);
        }
        finally { release.TrySetResult(); }
        Assert.False((await pause.WaitAsync(Deadline, Token)).Succeeded);
        await shutdown.WaitAsync(Deadline, Token);
        Assert.All(bus.Processors, p => Assert.Equal(1, p.Closes));
        Assert.All(consumer.GetStatus().Subscriptions, s => Assert.Equal(ConsumerState.Unavailable, s.State));
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static async Task<HttpResponseMessage> PostAsync(HttpClient http, string action, string? header, string? host = null)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"/control/{action}");
        request.Headers.Host = host;
        if (header is not null)
        {
            request.Headers.Add(ConsumerControlProtocol.HeaderName, header);
        }
        return await http.SendAsync(request, Token);
    }

    private sealed class FakeBus(int processorCount) : ServiceBusClient
    {
        public FakeProcessor[] Processors { get; } = Enumerable.Range(0, processorCount).Select(_ => new FakeProcessor()).ToArray();
        public List<(string Topic, string Subscription)> Entities { get; } = [];

        public override ServiceBusProcessor CreateProcessor(string topicName, string subscriptionName, ServiceBusProcessorOptions options)
        {
            var processor = Processors[Entities.Count];
            Entities.Add((topicName, subscriptionName));
            return processor;
        }
    }

    private sealed class FakeProcessor : ServiceBusProcessor
    {
        public int Starts { get; private set; }
        public int Stops { get; private set; }
        public int Closes { get; private set; }
        public bool FailStop { get; set; }
        public Task BeforeStart { get; set; } = Task.CompletedTask;
        public Task BeforeStop { get; set; } = Task.CompletedTask;
        private bool processing;
        public override bool IsProcessing => processing;

        public override async Task StartProcessingAsync(CancellationToken cancellationToken = default)
        {
            await BeforeStart.WaitAsync(cancellationToken);
            Assert.False(processing);
            Starts++;
            processing = true;
        }

        public override async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        {
            await BeforeStop.WaitAsync(cancellationToken);
            Assert.True(processing);
            Stops++;
            if (FailStop)
            {
                throw new InvalidOperationException("Simulated broker failure");
            }
            processing = false;
        }

        public override Task CloseAsync(CancellationToken cancellationToken = default)
        {
            Closes++;
            processing = false;
            return Task.CompletedTask;
        }
    }
}
