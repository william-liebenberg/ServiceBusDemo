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
    public async Task EachProcessorPostsTheBrokerDelivery(string consumerName, string broadcastSubscription)
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

        string[] routed = consumerName == "ConsumerOne" ? ["high-priority", "all"] : ["eu-orders"];
        var count = 2 + routed.Length;
        Assert.Equal(count, bus.Processors.Count);
        var before = DateTimeOffset.UtcNow;
        foreach (var processor in bus.Processors)
        {
            await processor.DeliverAsync(NewMessage(processor.Topic), token).WaitAsync(WaitLimit, token);
        }

        var requests = new List<Request>();
        for (var i = 0; i < count; i++)
        {
            requests.Add(await handler.NextAsync(token));
        }
        Assert.All(requests, request =>
        {
            Assert.Equal(HttpMethod.Post, request.Method);
            Assert.Equal("/api/deliveries", request.Uri.AbsolutePath);
            Assert.Equal("webfrontend.test", request.Uri.Host);
            Assert.Equal("application/json", request.MediaType);
            Assert.Equal(consumerName, request.Delivery.Consumer);
            Assert.Equal("order £42", request.Delivery.Body);
            Assert.Equal(4, request.Delivery.DeliveryCount);
            Assert.Equal(DeliveryOutcomes.Completed, request.Delivery.Outcome);
            Assert.Equal("Broker subject", request.Delivery.Subject);
            Assert.Equal("order-42", request.Delivery.CorrelationId);
            Assert.Equal("High", request.Delivery.ApplicationProperties!["Priority"]);
            Assert.Equal("2.5", request.Delivery.ApplicationProperties["Weight"]);
            Assert.InRange(request.Delivery.ReceivedAt, before, DateTimeOffset.UtcNow);
            Assert.InRange(request.Delivery.SettledAt, request.Delivery.ReceivedAt, DateTimeOffset.UtcNow);
            Assert.Equal(InitialLock, request.Delivery.InitialLockedUntil);
        });
        Assert.Equal(("CompetingConsumers", "business-events", "business-events", "business-events"), Routing(requests[0].Delivery));
        Assert.Equal(("PubSub", "broadcast-events", broadcastSubscription, "broadcast-events"), Routing(requests[1].Delivery));
        Assert.Equal(routed.Select(subscription => ("TopicFilters", "routed-events", subscription, "routed-events")),
            requests.Skip(2).Select(request => Routing(request.Delivery)));
        await consumer.StopAsync(token);
        await host.StopAsync(token);
        Assert.Equal(count, handler.Attempts);
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
        await processor.DeliverAsync(NewMessage("in-flight"), token).WaitAsync(token);
        await handler.NextAsync(token);
        for (var i = 0; i < 257; i++)
        {
            await processor.DeliverAsync(NewMessage($"queued-{i}"), token).WaitAsync(token);
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

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SettlementUsesBrokerAttemptsAndOnlyReportsSuccess(bool useConsumerOne)
    {
        var token = TestContext.Current.CancellationToken;
        using var handler = new RecordingHandler(_ => Task.FromResult(new HttpResponseMessage(HttpStatusCode.Accepted)));
        using var host = CreateHost(handler, new WarningLog());
        await host.StartAsync(token);
        var bus = new TestBusClient();
        using var consumer = CreateConsumer(useConsumerOne, bus, host);
        await consumer.StartAsync(token);
        await consumer.ExecuteTask!.WaitAsync(WaitLimit, token);

        foreach (var processor in bus.Processors)
        {
            var failFirst = new Dictionary<string, object> { [FailureLab.FailFirstCountProperty] = 2 };
            var poison = new Dictionary<string, object>(failFirst) { [FailureLab.AlwaysFailProperty] = true };
            await VerifyAsync(1, failFirst, DeliveryOutcomes.Abandoned);
            await VerifyAsync(2, failFirst, DeliveryOutcomes.Abandoned);
            await VerifyAsync(3, failFirst, DeliveryOutcomes.Completed);
            await VerifyAsync(4, poison, DeliveryOutcomes.Abandoned);
            await VerifyAsync(5, poison, DeliveryOutcomes.DeadLettered);
            await VerifyAsync(6, poison, DeliveryOutcomes.DeadLettered);

            using var delay = CancellationTokenSource.CreateLinkedTokenSource(token);
            var pending = processor.DeliverAsync(NewMessage("cancelled", properties: new() { [FailureLab.DelaySecondsProperty] = 120 }), delay.Token);
            Assert.False(pending.IsCompleted);
            delay.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.WaitAsync(WaitLimit, token));
            Assert.Equal(6, processor.Receiver.Settlements.Count);

            var error = new ServiceBusException("The lock was lost.", ServiceBusFailureReason.MessageLockLost);
            processor.Receiver.SettlementError = error;
            Assert.Same(error, await Assert.ThrowsAsync<ServiceBusException>(() => processor.DeliverAsync(NewMessage("lost"), token)));
            processor.Receiver.SettlementError = null;
            await VerifyAsync(1, [], DeliveryOutcomes.Completed, "after-error");

            async Task VerifyAsync(int attempt, Dictionary<string, object> options, string outcome, string id = "same-id")
            {
                await processor.DeliverAsync(NewMessage(id, attempt, options), token).WaitAsync(WaitLimit, token);
                var report = (await handler.NextAsync(token)).Delivery;
                Assert.Equal((id, attempt, outcome), (report.MessageId, report.DeliveryCount, report.Outcome));
                Assert.Equal(outcome, processor.Receiver.Settlements.Last());
            }
        }
        await consumer.StopAsync(token);
        await host.StopAsync(token);
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
        "business-events", "body", 1, DateTimeOffset.UnixEpoch, DeliveryOutcomes.Completed,
        DateTimeOffset.UnixEpoch.AddMinutes(1), DateTimeOffset.UnixEpoch);

    private static readonly DateTimeOffset InitialLock = DateTimeOffset.UnixEpoch.AddMinutes(1);

    private static ServiceBusReceivedMessage NewMessage(string id, int count = 4, Dictionary<string, object>? properties = null) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("order £42"), messageId: id,
            deliveryCount: count, subject: "Broker subject", correlationId: "order-42",
            properties: properties ?? new Dictionary<string, object> { ["Priority"] = "High", ["Weight"] = 2.5 },
            lockedUntil: InitialLock);

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
            Assert.False(options.AutoCompleteMessages);
            Assert.True(options.MaxConcurrentCalls >= 2);
            var processor = new TestProcessor(topicName);
            Processors.Add(processor);
            return processor;
        }
    }

    private sealed class TestProcessor(string topic) : ServiceBusProcessor
    {
        public TestReceiver Receiver { get; } = new();
        public string Topic => topic;
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override Task StopProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task DeliverAsync(ServiceBusReceivedMessage message, CancellationToken token = default) =>
            OnProcessMessageAsync(new ProcessMessageEventArgs(message, Receiver, token));
    }

    private sealed class TestReceiver : ServiceBusReceiver
    {
        public List<string> Settlements { get; } = [];
        public Exception? SettlementError { get; set; }

        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default) =>
            Settle(DeliveryOutcomes.Completed);

        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message,
            IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default) =>
            Settle(DeliveryOutcomes.Abandoned);

        public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, string deadLetterReason,
            string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default)
        {
            Assert.Equal(FailureLab.DeadLetterReason, deadLetterReason);
            return Settle(DeliveryOutcomes.DeadLettered);
        }

        private Task Settle(string outcome)
        {
            Settlements.Add(outcome);
            return SettlementError is { } error ? Task.FromException(error) : Task.CompletedTask;
        }
    }

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
