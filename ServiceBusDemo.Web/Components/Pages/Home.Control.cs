using Microsoft.AspNetCore.Components;
using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Web.Components.Pages;

public partial class Home
{
    [Inject] private ConsumerOneControlClient ConsumerOneControl { get; set; } = default!;
    [Inject] private ConsumerTwoControlClient ConsumerTwoControl { get; set; } = default!;

    private readonly ControlLane oneControl = new();
    private readonly ControlLane twoControl = new();

    private ControlLane LaneFor(string consumer) => consumer == BurstDeliveryTracker.ConsumerOneId ? oneControl : twoControl;
    private ConsumerControlClient ClientFor(string consumer) => consumer == BurstDeliveryTracker.ConsumerOneId ? ConsumerOneControl : ConsumerTwoControl;

    private async Task PollConsumersAsync(CancellationToken token)
    {
        // Independent loops keep one consumer's timeout from delaying the other lane.
        await Task.WhenAll(consumers.Select(consumer => PollConsumerAsync(consumer.Id, token)));
    }

    private async Task PollConsumerAsync(string consumer, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                await RefreshConsumerAsync(consumer, token);
                await Task.Delay(TimeSpan.FromSeconds(2), token);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // The page has been disposed.
        }
    }

    private async Task RefreshConsumerAsync(string consumer, CancellationToken token)
    {
        var lane = LaneFor(consumer);
        if (disposed || lane.Changing)
        {
            return;
        }

        var version = lane.Version;
        try
        {
            var status = await ClientFor(consumer).GetStatusAsync(token);
            if (!disposed && version == lane.Version)
            {
                var error = status.State switch
                {
                    ConsumerState.Mixed => "The subscriptions are in different states. Choose Pause or Resume to bring them into sync.",
                    ConsumerState.Unavailable => "The consumer is not ready for control. Check the consumer in Aspire; status will retry automatically.",
                    _ => null
                };
                var confirmed = lane.UnconfirmedTarget == status.State;
                var changed = lane.State != status.State || lane.RefreshError != error || confirmed;
                if (confirmed)
                {
                    lane.UnconfirmedTarget = null;
                }
                lane.State = status.State;
                lane.RefreshError = error;
                if (changed)
                {
                    StateHasChanged();
                }
            }
        }
        catch (Exception ex)
        {
            if (!disposed && version == lane.Version)
            {
                const string error = "Could not read consumer status. Check the consumer in Aspire; status will retry automatically.";
                if (lane.RefreshError != error)
                {
                    Logger.LogWarning(ex, "Failed to read {Consumer} status", consumer);
                    lane.State = ConsumerState.Unavailable;
                    lane.RefreshError = error;
                    StateHasChanged();
                }
            }
        }
    }

    private async Task ChangeConsumerAsync(string consumer, bool pause)
    {
        var lane = LaneFor(consumer);
        if (disposed || lane.ActionDisabled(pause))
        {
            return;
        }

        lane.Version++; // A status read already in flight must not overwrite this action.
        lane.Changing = true;
        lane.UnconfirmedTarget = null;
        lane.RefreshError = null;
        try
        {
            var client = ClientFor(consumer);
            var status = await (pause ? client.PauseAsync(pageLifetime.Token) : client.ResumeAsync(pageLifetime.Token));
            if (!disposed)
            {
                lane.State = status.State;
            }
        }
        catch (Exception ex)
        {
            if (!disposed)
            {
                var action = pause ? "pause" : "resume";
                Logger.LogWarning(ex, "Failed to {Action} {Consumer}", action, consumer);
                lane.State = ex is ConsumerTransitionException { Status.State: ConsumerState.Mixed }
                    ? ConsumerState.Mixed : ConsumerState.Unavailable;
                lane.UnconfirmedTarget = ActionTarget(pause);
            }
        }
        finally
        {
            lane.Changing = false;
        }
    }

    private static ConsumerState ActionTarget(bool pause) => pause ? ConsumerState.Paused : ConsumerState.Running;

    private sealed class ControlLane
    {
        public ConsumerState? State { get; set; }
        public bool Changing { get; set; }
        public long Version { get; set; }
        public ConsumerState? UnconfirmedTarget { get; set; }
        public string? RefreshError { get; set; }

        public string Badge => Changing ? "Updating…" : State?.ToString() ?? "Checking status…";

        public string? Error => UnconfirmedTarget is { } target
            ? $"Could not confirm {(target == ConsumerState.Paused ? "pause" : "resume")}. Check the consumer in Aspire and try again."
            : RefreshError;

        public bool ActionDisabled(bool pause) => Changing || State is null or ConsumerState.Initializing or ConsumerState.Transitioning
            || State == ActionTarget(pause);
    }
}
