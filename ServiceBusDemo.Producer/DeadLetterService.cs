using Azure.Messaging.ServiceBus;
using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Producer;

public sealed record DeadLetterSubscription(string Topic, string Subscription);
public sealed record DeadLetterEntry(long SequenceNumber, string MessageId, string? DeadLetterReason,
    string? DeadLetterErrorDescription, string Body, IReadOnlyDictionary<string, string> ApplicationProperties, int DeliveryCount);
public sealed record DeadLetterPage(DeadLetterEntry[] Messages, long? NextFromSequenceNumber);
public sealed record DeadLetterActionResult(string? PublishedMessageId);

public sealed class DeadLetterService([FromKeyedServices(DeadLetterService.ClientKey)] ServiceBusClient bus,
    ILogger<DeadLetterService> logger)
{
    public const string ClientKey = "dead-letter-recovery";
    public const int PageSize = 20;
    private const int BatchSize = 10;
    private const int MaxBatches = 5;
    private const string DuplicateWarning = "Refresh and investigate before retrying; another resubmit can produce duplicates.";

    public async Task<IResult> PeekAsync(DeadLetterSubscription pair, long? fromSequenceNumber, CancellationToken token)
    {
        try
        {
            await using var receiver = CreateReceiver(pair);
            var entries = await receiver.PeekMessagesAsync(PageSize, fromSequenceNumber ?? 0, token);
            // The broker can return fewer than requested even when more messages exist.
            long? next = entries.Count > 0 && entries[^1].SequenceNumber < long.MaxValue
                ? entries[^1].SequenceNumber + 1 : null;
            return Results.Ok(new DeadLetterPage(entries.Select(message => new DeadLetterEntry(
                message.SequenceNumber, message.MessageId, message.DeadLetterReason, message.DeadLetterErrorDescription,
                message.Body.ToString(), message.ApplicationProperties.ToDictionary(p => p.Key,
                    p => Convert.ToString(p.Value, System.Globalization.CultureInfo.InvariantCulture) ?? ""),
                message.DeliveryCount)).ToArray(),
                next));
        }
        catch (Exception ex) when (!token.IsCancellationRequested)
        {
            logger.LogWarning(ex, "Could not peek dead letters for {Pair}", pair);
            return Error(502, $"Could not peek dead letters: {ex.Message}");
        }
    }

    public async Task<IResult> ActAsync(DeadLetterSubscription pair, long sequenceNumber, ServiceBusSender? sender,
        CancellationToken token)
    {
        var held = new List<ServiceBusReceivedMessage>();
        ServiceBusReceiver? receiver = null;
        ServiceBusReceivedMessage? target = null;
        var completed = false;
        var phase = "search";
        string? publishedId = null;
        try
        {
            using var search = CancellationTokenSource.CreateLinkedTokenSource(token);
            search.CancelAfter(TimeSpan.FromSeconds(8));
            receiver = CreateReceiver(pair);
            for (var i = 0; i < MaxBatches && target is null; i++)
            {
                var batch = await receiver.ReceiveMessagesAsync(BatchSize, TimeSpan.FromSeconds(1), search.Token);
                foreach (var message in batch)
                {
                    if (target is null && message.SequenceNumber == sequenceNumber)
                        target = message;
                    else
                        held.Add(message);
                }
                if (batch.Count == 0) break;
            }
            if (target is null) return Unavailable();

            using var action = CancellationTokenSource.CreateLinkedTokenSource(token);
            action.CancelAfter(TimeSpan.FromSeconds(10));
            action.Token.ThrowIfCancellationRequested();
            if (sender is not null)
            {
                var copy = new ServiceBusMessage(target.Body)
                {
                    MessageId = Guid.NewGuid().ToString(),
                    Subject = target.Subject,
                    CorrelationId = target.CorrelationId,
                    ContentType = target.ContentType
                };
                foreach (var (key, value) in target.ApplicationProperties)
                {
                    if (!key.Equals(FailureLab.AlwaysFailProperty, StringComparison.OrdinalIgnoreCase))
                        copy.ApplicationProperties.Add(key, value);
                }

                phase = "publish";
                await sender.SendMessageAsync(copy, action.Token);
                publishedId = copy.MessageId;
            }

            phase = "completion";
            await receiver.CompleteMessageAsync(target, action.Token);
            completed = true;
            return Results.Ok(new DeadLetterActionResult(publishedId));
        }
        catch (OperationCanceledException) when (phase == "search" && !token.IsCancellationRequested)
        {
            return Unavailable();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Dead-letter {Phase} failed for {Pair}, sequence {SequenceNumber}", phase, pair, sequenceNumber);
            return phase switch
            {
                "publish" => Error(502, $"Outcome uncertain: publish was not confirmed and the original was not completed. {DuplicateWarning} {ex.Message}"),
                "completion" when publishedId is not null => Error(502, $"Outcome uncertain: copy {publishedId} was published but completion of the original was not confirmed. {DuplicateWarning} {ex.Message}"),
                "completion" => Error(502, $"Outcome uncertain: purge completion was not confirmed. Refresh before retrying. {ex.Message}"),
                _ => Error(502, $"Dead-letter action stopped before settlement: {ex.Message}")
            };
        }
        finally
        {
            if (receiver is not null)
            {
                // Do not use the request token: even a disconnected caller must release other locks.
                if (target is not null && !completed) held.Add(target);
                using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                foreach (var message in held)
                {
                    try { await receiver.AbandonMessageAsync(message, cancellationToken: cleanup.Token); }
                    catch (Exception ex) { logger.LogWarning(ex, "Could not abandon dead-letter lock for sequence {SequenceNumber}; it may expire", message.SequenceNumber); }
                }
                try { await receiver.DisposeAsync(); }
                catch (Exception ex) { logger.LogWarning(ex, "Could not close dead-letter receiver for {Pair}", pair); }
            }
        }

        static IResult Unavailable() => Error(409,
            "The selected dead letter could not be locked within the bounded search. Nothing was completed. Refresh and try again.");
    }

    private ServiceBusReceiver CreateReceiver(DeadLetterSubscription pair) => bus.CreateReceiver(pair.Topic, pair.Subscription,
        new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter, ReceiveMode = ServiceBusReceiveMode.PeekLock });

    private static IResult Error(int status, string message) => Results.Text(message, statusCode: status);
}
