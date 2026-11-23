using System.Collections.Concurrent;
using System.Net.Http.Json;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Tests;

public class DeliveryReportingTests
{
    private static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData("ConsumerOne", "consumer-one")]
    [InlineData("ConsumerTwo", "consumer-two")]
    public async Task BothProcessorsPostTheBrokerDelivery(string consumerName, string broadcastSubscription)
    {
        var token = TestContext.Current.CancellationToken;
        using var handler = new RecordingHandler(_ => Task.FromResult(new HttpResponseMessage(HttpStatusCode.Accepted)));
        var logs = new WarningLog();
        using var host = CreateHost(handler, logs);
        await host.StartAsync(token);
        var bus = new TestBusClient();
        using var consumer = CreateConsumer(consumerName == "ConsumerOne", bus, host);
        await consumer.StartAsync(token);
        await consumer.ExecuteTask!.WaitAsync(WaitLimit, token);

        Assert.Equal(2, bus.Processors.Count);
        var before = DateTimeOffset.UtcNow;
        foreach (var processor in bus.Processors)
        {
            await processor.DeliverAsync(NewMessage(processor.Topic)).WaitAsync(WaitLimit, token);
        }

        var requests = new[] { await handler.NextAsync(token), await handler.NextAsync(token) };
        Assert.All(requests, request =>
        {
            Assert.Equal(HttpMethod.Post, request.Method);
            Assert.Equal("/api/deliveries", request.Uri.AbsolutePath);
            Assert.Equal("webfrontend.test", request.Uri.Host);
            Assert.Equal("application/json", request.MediaType);
            Assert.Equal(consumerName, request.Delivery.Consumer);
            Assert.Equal("order £42", request.Delivery.Body);
            Assert.Equal(4, request.Delivery.DeliveryCount);
            Assert.InRange(request.Delivery.ReceivedAt, before, DateTimeOffset.UtcNow);
        });
        Assert.Equal(("CompetingConsumers", "business-events", "business-events", "business-events"), Routing(requests[0].Delivery));
        Assert.Equal(("PubSub", "broadcast-events", broadcastSubscription, "broadcast-events"), Routing(requests[1].Delivery));
        await consumer.StopAsync(token);
        await host.StopAsync(token);
        Assert.Equal(2, handler.Attempts);
        Assert.Empty(logs.Messages);
    }

    [Theory]
    [InlineData("unavailable", "returned HTTP 503")]
    [InlineData("network", "failed")]
    [InlineData("timeout", "timed out")]
    public async Task FailedAttemptWarnsOnceAndWorkerContinues(string failure, string warning)
    {
        var token = TestContext.Current.CancellationToken;
        var attempts = 0;
        using var handler = new RecordingHandler(async cancellationToken =>
        {
            if (Interlocked.Increment(ref attempts) > 1)
            {
                return new HttpResponseMessage(HttpStatusCode.Accepted);
            }

            switch (failure)
            {
                case "unavailable": return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
                case "network": throw new HttpRequestException("Connection refused");
                default:
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                    throw new InvalidOperationException("Expected the request deadline to cancel the handler.");
            }
        });
        var logs = new WarningLog();
        using var host = CreateHost(handler, logs);
        await host.StartAsync(token);
        using (var client = host.Services.GetRequiredService<IHttpClientFactory>().CreateClient(nameof(DeliveryReporter)))
        {
            Assert.Equal(TimeSpan.FromSeconds(2), client.Timeout);
            Assert.Equal(new Uri("https+http://webfrontend"), client.BaseAddress);
        }

        var queue = host.Services.GetRequiredService<DeliveryReportQueue>();
        queue.Enqueue(Example("failed-delivery"));
        queue.Enqueue(Example("next-delivery"));
        Assert.Equal("failed-delivery", (await handler.NextAsync(token)).Delivery.MessageId);
        Assert.Equal("next-delivery", (await handler.NextAsync(token)).Delivery.MessageId);
        await host.StopAsync(token);

        Assert.Equal(2, handler.Attempts);
        var message = Assert.Single(logs.Messages);
        Assert.Contains("failed-delivery", message);
        Assert.Contains(warning, message);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MessageCallbacksDoNotWaitForHttpAndShutdownDiscardsBacklog(bool useConsumerOne)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(WaitLimit);
        var token = deadline.Token;
        var requestCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var handler = new RecordingHandler(async cancellationToken =>
        {
            try { await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken); }
            finally { requestCancelled.TrySetResult(); }
            return new HttpResponseMessage(HttpStatusCode.Accepted);
        });
        var logs = new WarningLog();
        // Disable the HTTP deadline in this test so only host shutdown can cancel the request.
        using var host = CreateHost(handler, logs, client => client.Timeout = Timeout.InfiniteTimeSpan);
        await host.StartAsync(token);
        var bus = new TestBusClient();
        using var consumer = CreateConsumer(useConsumerOne, bus, host);
        await consumer.StartAsync(token);
        await consumer.ExecuteTask!.WaitAsync(token);
        var processor = bus.Processors[0];
        await processor.DeliverAsync(NewMessage("in-flight")).WaitAsync(token);
        await handler.NextAsync(token);
        for (var i = 0; i < 257; i++)
        {
            await processor.DeliverAsync(NewMessage($"queued-{i}")).WaitAsync(token);
        }

        Assert.Equal(1, handler.Attempts);
        Assert.False(requestCancelled.Task.IsCompleted);
        Assert.Contains("Dropping delivery report for queued-256", Assert.Single(logs.Messages));
        await consumer.StopAsync(token);
        await host.StopAsync(token);
        await requestCancelled.Task.WaitAsync(token);
        Assert.Equal(1, handler.Attempts);
        Assert.Single(logs.Messages);
    }

    private static BackgroundService CreateConsumer(bool useConsumerOne, TestBusClient bus, IHost host)
    {
        var queue = host.Services.GetRequiredService<DeliveryReportQueue>();
        var configuration = new ConfigurationBuilder().Build();
        return useConsumerOne
            ? new ConsumerOne.MessageConsumer(bus, configuration, NullLogger<ConsumerOne.MessageConsumer>.Instance, queue)
            : new ConsumerTwo.MessageConsumer(bus, configuration, NullLogger<ConsumerTwo.MessageConsumer>.Instance, queue);
    }

    private static (string, string, string, string) Routing(DeliveryEvent delivery) =>
        (delivery.Pattern, delivery.Topic, delivery.Subscription, delivery.MessageId);

    private static DeliveryEvent Example(string id) => new(id, "CompetingConsumers", "ConsumerOne", "business-events",
        "business-events", "body", 1, DateTimeOffset.UtcNow);

    private static ServiceBusReceivedMessage NewMessage(string id) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("order £42"), messageId: id, deliveryCount: 4);

    private static IHost CreateHost(RecordingHandler handler, WarningLog logs, Action<HttpClient>? configure = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.AddServiceDefaults();
        builder.Configuration["services:webfrontend:http:0"] = "http://webfrontend.test";
        builder.Logging.AddProvider(logs);
        var client = builder.Services.AddDeliveryReporting().ConfigurePrimaryHttpMessageHandler(() => handler);
        if (configure is not null)
        {
            client.ConfigureHttpClient(configure);
        }
        return builder.Build();
    }

    private sealed class TestBusClient : ServiceBusClient
    {
        public List<TestProcessor> Processors { get; } = [];

        public override ServiceBusProcessor CreateProcessor(string topicName, string subscriptionName, ServiceBusProcessorOptions options)
        {
            Assert.True(options.AutoCompleteMessages);
            Assert.Equal(1, options.MaxConcurrentCalls);
            var processor = new TestProcessor(topicName);
            Processors.Add(processor);
            return processor;
        }
    }

    private sealed class TestProcessor(string topic) : ServiceBusProcessor
    {
        private readonly TestReceiver _receiver = new();
        public string Topic => topic;
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override Task StopProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task DeliverAsync(ServiceBusReceivedMessage message) => OnProcessMessageAsync(new ProcessMessageEventArgs(
            message, _receiver, new CancellationToken(canceled: true)));
    }

    private sealed class TestReceiver : ServiceBusReceiver;

    private sealed record Request(HttpMethod Method, Uri Uri, string? MediaType, DeliveryEvent Delivery);

    private sealed class RecordingHandler(Func<CancellationToken, Task<HttpResponseMessage>> respond) : HttpMessageHandler
    {
        private readonly Channel<Request> _requests = Channel.CreateUnbounded<Request>();
        private int _attempts;
        public int Attempts => Volatile.Read(ref _attempts);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _attempts);
            var content = request.Content!;
            var delivery = await content.ReadFromJsonAsync<DeliveryEvent>(cancellationToken);
            _requests.Writer.TryWrite(new(request.Method, request.RequestUri!, content.Headers.ContentType?.MediaType, delivery!));
            return await respond(cancellationToken);
        }

        public Task<Request> NextAsync(CancellationToken token) => _requests.Reader.ReadAsync(token).AsTask().WaitAsync(WaitLimit, token);
    }

    private sealed class WarningLog : ILoggerProvider, ILogger
    {
        public ConcurrentQueue<string> Messages { get; } = new();
        public ILogger CreateLogger(string categoryName) => categoryName.StartsWith("ServiceBusDemo.", StringComparison.Ordinal)
            ? this : NullLogger.Instance;
        public bool IsEnabled(LogLevel logLevel) => logLevel == LogLevel.Warning;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel)) EnqueueWarning(formatter(state, exception));
        }
        private void EnqueueWarning(string message) => Messages.Enqueue(message);
        public void Dispose() { }
    }
}
