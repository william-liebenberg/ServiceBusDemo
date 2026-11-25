using System.Diagnostics;
using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using Microsoft.AspNetCore.Components.Routing;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.JSInterop;

namespace ServiceBusDemo.Web.Components.Pages;

public partial class Home
{
    private const string BurstNavigationWarning = "Leaving or reloading during a burst can interrupt sending and lose this page’s progress. " +
        "The producer may still accept an in-flight message without this page receiving confirmation.";

    [Inject] private IJSRuntime JS { get; set; } = default!;
    private readonly CancellationTokenSource pageLifetime = new();
    private BurstDeliveryTracker? burst;
    private CancellationTokenSource? burstDelay;
    private string burstCount = "10";
    private string burstRate = "10";
    private bool fixedRate;
    private bool cancelPending;
    private bool burstFailed;
    private string? burstStatus;
    private string? burstAnnouncement;
    private (int Sent, int One, int Two)? announcedCounts;
    private bool announcementPending;
    private string? countError;
    private string? rateError;
    private ElementReference burstAction;
    private InputTextArea messageInput = default!;
    private (ElementReference Element, bool PreventScroll)? focusAfterRender;

    private bool Busy => isSending || burstDelay is not null;

    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (firstRender && !disposed)
        {
            // Only poll once the page is interactive, not during prerendering.
            _ = PollConsumersAsync(pageLifetime.Token);
        }

        QueueDepthAnnouncement();

        if (focusAfterRender is not { } target || disposed)
        {
            return;
        }

        focusAfterRender = null;
        try
        {
            await target.Element.FocusAsync(preventScroll: target.PreventScroll);
        }
        catch (JSDisconnectedException)
        {
            // The browser can disconnect between rendering and moving focus.
        }
    }

    private void QueueAnnouncements(int sent, int one, int two)
    {
        if (disposed || announcementPending || (announcedCounts == (sent, one, two) && pendingDeliveryReports == 0))
        {
            return;
        }

        announcementPending = true;
        _ = AnnounceCountsAsync(pageLifetime.Token);
    }

    private async Task AnnounceCountsAsync(CancellationToken cancellationToken)
    {
        try
        {
            // Coalesce rapid updates, then read the latest burst so even the final or late counts are announced.
            await Task.Delay(TimeSpan.FromMilliseconds(750), cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        finally
        {
            announcementPending = false;
        }

        if (!disposed && burst is { } tracker)
        {
            var (sent, one, two, _) = tracker.Snapshot();
            if (announcedCounts != (sent, one, two))
            {
                announcedCounts = (sent, one, two);
                burstAnnouncement = $"Burst: {sent} of {tracker.Requested} sends acknowledged. Reported deliveries: {consumers[0].Label} {one}, {consumers[1].Label} {two}.";
            }

            AnnounceDeliveryStatus(pendingDeliveryReports);
            pendingDeliveryReports = 0;

            StateHasChanged();
        }
    }

    private async Task SendBurstAsync()
    {
        if (disposed || Busy)
        {
            return;
        }

        var messageValid = await messageContext.ValidateAsync();
        if (disposed || Busy)
        {
            return;
        }

        var countValid = TryBoundedInteger(burstCount, 500, out var count);
        var rate = 0;
        var rateValid = !fixedRate || TryBoundedInteger(burstRate, 1000, out rate);
        countError = countValid ? null : "Enter a whole number from 1 to 500.";
        rateError = rateValid ? null : "Enter a whole number from 1 to 1000.";
        if (!messageValid || !countValid || !rateValid)
        {
            if (!messageValid && messageInput.Element is { } field)
            {
                focusAfterRender = (field, false);
            }
            return;
        }

        var message = model.Message!.Trim();
        var pattern = model.Pattern;
        burst?.Dispose();
        var tracker = new BurstDeliveryTracker(Feed, pattern, count);
        burst = tracker;
        burstAnnouncement = null;
        announcedCounts = null;
        statusMessage = null;
        burstFailed = false;
        burstStatus = "Sending burst…";
        cancelPending = false;
        using var delay = new CancellationTokenSource();
        burstDelay = delay;
        focusAfterRender = (burstAction, true);
        var requestToken = pageLifetime.Token;
        var spacing = rate == 0 ? TimeSpan.Zero : TimeSpan.FromTicks((long)Math.Ceiling((double)TimeSpan.TicksPerSecond / rate));
        long previousResponse = 0;

        try
        {
            for (var index = 0; index < count; index++)
            {
                if (index > 0 && spacing > TimeSpan.Zero)
                {
                    // Wait from the acknowledgement, not from client invocation: request setup can delay the actual start.
                    while (spacing - Stopwatch.GetElapsedTime(previousResponse) is { Ticks: > 0 } remaining)
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(Math.Ceiling(remaining.TotalMilliseconds)), delay.Token);
                    }
                }

                if (disposed || delay.IsCancellationRequested)
                {
                    break;
                }

                tracker.BeginRequest();
                SendMessageResult result;
                try
                {
                    result = await ProducerApi.SendMessageAsync(message, pattern, requestToken);
                    previousResponse = Stopwatch.GetTimestamp();
                    if (string.IsNullOrWhiteSpace(result.MessageId))
                    {
                        throw new InvalidOperationException("Producer API returned no message ID.");
                    }
                }
                catch (Exception ex)
                {
                    tracker.EndRequest();
                    if (!disposed)
                    {
                        Logger.LogError(ex, "Failed to send burst message via producer API");
                        burstFailed = true;
                        burstStatus = $"Burst stopped: {ex.Message} The producer may have accepted the last request despite the missing acknowledgement.";
                    }
                    return;
                }

                if (disposed)
                {
                    return;
                }

                tracker.EndRequest(result.MessageId);
                sentMessages.Insert(0, new SentMessage(result.MessageId, pattern, result.Topic, message, DateTimeOffset.UtcNow));
                StateHasChanged();
                // Allow progress rendering and cancellation even with synchronously completed requests.
                await Task.Yield();
            }

            burstStatus = tracker.Snapshot().Sent == count ? "Burst complete." : "Burst cancelled.";
        }
        catch (OperationCanceledException) when (delay.IsCancellationRequested)
        {
            burstStatus = "Burst cancelled.";
        }
        finally
        {
            burstDelay = null;
            cancelPending = false;
        }
    }

    private async Task ConfirmBurstNavigationAsync(LocationChangingContext navigation)
    {
        if (burstDelay is null)
        {
            return;
        }

        try
        {
            if (!await JS.InvokeAsync<bool>("confirm", navigation.CancellationToken,
                $"{BurstNavigationWarning}\n\nLeave this page?"))
            {
                navigation.PreventNavigation();
            }
        }
        catch (JSDisconnectedException)
        {
            // The browser disconnected before it could confirm navigation.
            navigation.PreventNavigation();
        }
    }

    private void CancelBurst()
    {
        if (burstDelay is not { } delay || cancelPending)
        {
            return;
        }

        cancelPending = true;
        burstStatus = "Cancellation pending. If a request is in flight, waiting for it to finish…";
        // User cancellation must not abort an in-flight producer request.
        delay.Cancel();
    }

    private static bool TryBoundedInteger(string input, int max, out int value) =>
        int.TryParse(input.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out value) && value >= 1 && value <= max;
}
