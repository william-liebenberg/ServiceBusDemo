using System.Net.Http.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using ServiceBusDemo.Producer;
using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Tests;

public class ProducerDeadLetterTests
{
    private const string Query = "?topic=routed-custom&subscription=priority-custom";
    private const string Messages = "/dead-letters/messages";

    [Fact]
    public async Task TopologyRejectsInvalidPairsAndPeekUsesBoundedSequencePages()
    {
        var token = TestContext.Current.CancellationToken;
        var broker = new Broker();
        broker.Entries = Enumerable.Range(1, 24).Select(i => Entry(i * 2)).ToArray();
        await using var factory = CreateFactory(broker);
        using var http = factory.CreateClient(new() { BaseAddress = new Uri("https://localhost") });
        var pairs = await http.GetFromJsonAsync<DeadLetterSubscription[]>("/dead-letters/subscriptions", token);
        Assert.Equal(new DeadLetterSubscription[] { new("business-events", "business-events"), new("broadcast-events", "consumer-one"),
            new("broadcast-events", "consumer-two"), new("routed-custom", "priority-custom"), new("routed-custom", "eu-orders"),
            new("routed-custom", "all") }, pairs);
        const string invalid = "?topic=business-events&subscription=priority-custom";
        using var rejected = await http.GetAsync(Messages + invalid, token);
        Assert.Equal(HttpStatusCode.BadRequest, rejected.StatusCode);
        foreach (var action in new[] { "purge", "resubmit" })
        {
            using var response = await http.PostAsync($"{Messages}/2/{action}{invalid}", null, token);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        }
        Assert.Empty(broker.Calls);
        var first = (await http.GetFromJsonAsync<DeadLetterPage>(Messages + Query, token))!;
        var repeated = (await http.GetFromJsonAsync<DeadLetterPage>(Messages + Query, token))!;
        var next = (await http.GetFromJsonAsync<DeadLetterPage>(Messages + Query + $"&fromSequenceNumber={first.NextFromSequenceNumber}", token))!;
        Assert.Equal(20, first.Messages.Length);
        Assert.Equal(first.Messages.Select(message => message.SequenceNumber), repeated.Messages.Select(message => message.SequenceNumber));
        Assert.Equal(new long[] { 42, 44, 46, 48 }, next.Messages.Select(message => message.SequenceNumber));
        Assert.Equal(49L, next.NextFromSequenceNumber);
        var item = first.Messages[0];
        Assert.Equal(("same-id", "poison", "description", "body", 5),
            (item.MessageId, item.DeadLetterReason, item.DeadLetterErrorDescription, item.Body, item.DeliveryCount));
        Assert.Equal("High", item.ApplicationProperties["Priority"]);
        Assert.Equal(new[] { "peek:20:0", "dispose", "peek:20:0", "dispose", "peek:20:41", "dispose" }, broker.Calls);
    }

    [Fact]
    public async Task ShortBrokerPageOffersContinuationUntilAnEmptyPeek()
    {
        var token = TestContext.Current.CancellationToken;
        var broker = new Broker { Entries = [Entry(7), Entry(42)], PeekLimit = 1 };
        await using var factory = CreateFactory(broker);
        using var http = factory.CreateClient(new() { BaseAddress = new Uri("https://localhost") });

        var first = (await http.GetFromJsonAsync<DeadLetterPage>(Messages + Query, token))!;
        Assert.Equal(7L, Assert.Single(first.Messages).SequenceNumber);
        Assert.Equal(8L, first.NextFromSequenceNumber);
        var next = (await http.GetFromJsonAsync<DeadLetterPage>(Messages + Query + $"&fromSequenceNumber={first.NextFromSequenceNumber}", token))!;
        Assert.Equal(42L, Assert.Single(next.Messages).SequenceNumber);
        var end = (await http.GetFromJsonAsync<DeadLetterPage>(Messages + Query + $"&fromSequenceNumber={next.NextFromSequenceNumber}", token))!;
        Assert.Empty(end.Messages);
        Assert.Null(end.NextFromSequenceNumber);
        Assert.Equal(new[] { "peek:20:0", "dispose", "peek:20:8", "dispose", "peek:20:43", "dispose" }, broker.Calls);
    }

    [Theory]
    [InlineData("resubmit", null, 200, new[] { "receive", "receive", "send", "complete:7", "abandon:1", "abandon:9", "dispose" })]
    [InlineData("purge", null, 200, new[] { "receive", "receive", "complete:7", "abandon:1", "abandon:9", "dispose" })]
    [InlineData("resubmit", "send", 502, new[] { "receive", "receive", "send", "abandon:1", "abandon:9", "abandon:7", "dispose" })]
    [InlineData("resubmit", "complete", 502, new[] { "receive", "receive", "send", "complete:7", "abandon:1", "abandon:9", "abandon:7", "dispose" })]
    [InlineData("purge", "complete", 502, new[] { "receive", "receive", "complete:7", "abandon:1", "abandon:9", "abandon:7", "dispose" })]
    public async Task ActionsSettleOnlyExactSequenceAndSurfaceUncertainty(string action, string? failure, int status, string[] calls)
    {
        var token = TestContext.Current.CancellationToken;
        var broker = new Broker { Failure = failure };
        broker.Batches.Enqueue([Entry(1)]);
        broker.Batches.Enqueue([Entry(7), Entry(9)]);
        await using var factory = CreateFactory(broker);
        using var http = factory.CreateClient(new() { BaseAddress = new Uri("https://localhost") });
        using var response = await http.PostAsync($"{Messages}/7/{action}{Query}", null, token);
        Assert.Equal(status, (int)response.StatusCode);
        Assert.Equal(calls, broker.Calls);
        if (failure is not null)
        {
            var text = await response.Content.ReadAsStringAsync(token);
            Assert.Contains("Outcome uncertain", text);
            if (action == "resubmit") Assert.Contains("duplicates", text);
        }
        if (action == "resubmit")
        {
            var copy = Assert.IsType<ServiceBusMessage>(broker.Copy);
            Assert.NotEqual("same-id", copy.MessageId);
            Assert.Equal(("body", "subject", "correlation", "text/plain"), (copy.Body.ToString(), copy.Subject, copy.CorrelationId, copy.ContentType));
            Assert.Equal(5, copy.ApplicationProperties.Count);
            Assert.DoesNotContain(copy.ApplicationProperties.Keys, key => key.Equals(FailureLab.AlwaysFailProperty, StringComparison.OrdinalIgnoreCase));
            Assert.Equal("poison", copy.ApplicationProperties["DeadLetterReason"]);
            Assert.Equal("description", copy.ApplicationProperties["DeadLetterErrorDescription"]);
            Assert.Equal("High", copy.ApplicationProperties["Priority"]);
            Assert.Equal(2, copy.ApplicationProperties[FailureLab.FailFirstCountProperty]);
            Assert.Equal(4, copy.ApplicationProperties[FailureLab.DelaySecondsProperty]);
            if (failure is null)
                Assert.Equal(copy.MessageId, (await response.Content.ReadFromJsonAsync<DeadLetterActionResult>(token))!.PublishedMessageId);
        }
        else Assert.Null(broker.Copy);
    }

    [Fact]
    public async Task MissingTargetStopsAtSearchLimitAndAbandonsAllLocksEvenIfOneFails()
    {
        var token = TestContext.Current.CancellationToken;
        var broker = new Broker { Failure = "abandon" };
        for (var i = 1; i <= 6; i++) broker.Batches.Enqueue([Entry(i)]);
        await using var factory = CreateFactory(broker);
        using var http = factory.CreateClient(new() { BaseAddress = new Uri("https://localhost") });
        using var response = await http.PostAsync($"{Messages}/6/resubmit{Query}", null, token);
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Contains("Nothing was completed", await response.Content.ReadAsStringAsync(token));
        Assert.Equal(Enumerable.Repeat("receive", 5).Concat(Enumerable.Range(1, 5).Select(i => $"abandon:{i}")).Append("dispose"), broker.Calls);
        Assert.Null(broker.Copy);
    }

    private static ServiceBusReceivedMessage Entry(long sequence) => ServiceBusModelFactory.ServiceBusReceivedMessage(
        body: BinaryData.FromString("body"), sequenceNumber: sequence, messageId: "same-id", subject: "subject",
        correlationId: "correlation", contentType: "text/plain", deliveryCount: 5, properties: new Dictionary<string, object>
        {
            ["DeadLetterReason"] = "poison", ["DeadLetterErrorDescription"] = "description", ["Priority"] = "High",
            ["ALWAYS-FAIL"] = true, [FailureLab.FailFirstCountProperty] = 2, [FailureLab.DelaySecondsProperty] = 4
        });

    private static WebApplicationFactory<ProducerApiMarker> CreateFactory(Broker broker) =>
        new WebApplicationFactory<ProducerApiMarker>().WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Testing");
            builder.UseSetting("ConnectionStrings:messaging", "Endpoint=sb://localhost/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==");
            builder.UseSetting("ServiceBus:RoutedTopicName", "routed-custom");
            builder.UseSetting("ServiceBus:HighPrioritySubscriptionName", "priority-custom");
            builder.ConfigureServices(services =>
            {
                services.AddKeyedSingleton<ServiceBusClient>(DeadLetterService.ClientKey, broker);
                services.AddKeyedSingleton<ServiceBusSender>((DeadLetterService.ClientKey, "routed-events"), new Sender(broker));
            });
        });

    private sealed class Broker : ServiceBusClient
    {
        public List<string> Calls { get; } = [];
        public Queue<ServiceBusReceivedMessage[]> Batches { get; } = new();
        public ServiceBusReceivedMessage[] Entries = [];
        public int PeekLimit { get; init; } = int.MaxValue;
        public string? Failure { get; init; }
        public ServiceBusMessage? Copy;
        public override ServiceBusReceiver CreateReceiver(string topicName, string subscriptionName, ServiceBusReceiverOptions options)
        {
            Assert.Equal(("routed-custom", "priority-custom", SubQueue.DeadLetter, ServiceBusReceiveMode.PeekLock),
                (topicName, subscriptionName, options.SubQueue, options.ReceiveMode));
            return new Receiver(this);
        }
    }

    private sealed class Sender(Broker broker) : ServiceBusSender
    {
        public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
        {
            broker.Calls.Add("send");
            broker.Copy = message;
            if (broker.Failure == "send") throw new TimeoutException("send timed out");
            return Task.CompletedTask;
        }
    }

    private sealed class Receiver(Broker broker) : ServiceBusReceiver
    {
        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekMessagesAsync(int maxMessages, long? fromSequenceNumber = null,
            CancellationToken cancellationToken = default)
        {
            broker.Calls.Add($"peek:{maxMessages}:{fromSequenceNumber}");
            return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(broker.Entries
                .Where(e => e.SequenceNumber >= fromSequenceNumber).Take(Math.Min(maxMessages, broker.PeekLimit)).ToArray());
        }
        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveMessagesAsync(int maxMessages, TimeSpan? maxWaitTime = null,
            CancellationToken cancellationToken = default)
        {
            Assert.Equal(10, maxMessages);
            Assert.Equal(TimeSpan.FromSeconds(1), maxWaitTime);
            broker.Calls.Add("receive");
            return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(broker.Batches.TryDequeue(out var batch) ? batch : []);
        }
        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        {
            broker.Calls.Add($"complete:{message.SequenceNumber}");
            if (broker.Failure == "complete") throw new TimeoutException("complete timed out");
            return Task.CompletedTask;
        }
        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null,
            CancellationToken cancellationToken = default)
        {
            broker.Calls.Add($"abandon:{message.SequenceNumber}");
            if (broker.Failure == "abandon" && message.SequenceNumber == 1) throw new TimeoutException("abandon timed out");
            return Task.CompletedTask;
        }
        public override ValueTask DisposeAsync()
        {
            broker.Calls.Add("dispose");
            return ValueTask.CompletedTask;
        }
    }
}
