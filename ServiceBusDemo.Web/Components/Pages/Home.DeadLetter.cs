using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.JSInterop;

namespace ServiceBusDemo.Web.Components.Pages;

public partial class Home
{
    private const string DeadLetterRecoveryGuidance = "Refresh the subscription and investigate whether a copy was published or processed before retrying. Retrying a resubmit can create duplicates, even if the original is still listed.";
    private const string DeadLetterNavigationWarning = "Leaving or reloading during a dead-letter action can lose its result. The action may already have changed the broker. " + DeadLetterRecoveryGuidance;

    private CancellationTokenSource? deadLetterSession;
    private DeadLetterSubscription[] deadLetterSubscriptions = [];
    private DeadLetterSubscription? deadLetterSelection;
    private DeadLetterPage? deadLetterPage;
    private readonly Stack<long?> deadLetterHistory = new();
    private long? deadLetterCursor;
    private long deadLetterVersion;
    private bool deadLetterLoading;
    private bool deadLetterActing;
    private string? deadLetterLoadError;
    private string? deadLetterActionError;
    private string? deadLetterActionStatus;
    private (DeadLetterSubscription Pair, long Sequence)? pendingPurge;
    private (DeadLetterSubscription Pair, long Sequence)? focusedDeadLetter;
    private bool deadLetterNextFocused;
    private ElementReference messagingTab;
    private ElementReference deadLetterTab;
    private ElementReference deadLetterResult;
    private ElementReference purgeCancel;
    private bool focusPurgeCancel;
    private bool DeadLetterActive => deadLetterSession is not null;
    private bool TabSwitchBlocked => deadLetterActing || Busy;

    private async Task OpenDeadLettersAsync()
    {
        if (disposed || DeadLetterActive || TabSwitchBlocked) return;
        deadLetterSession = CancellationTokenSource.CreateLinkedTokenSource(pageLifetime.Token);
        var token = deadLetterSession.Token;
        _ = PollDeadLettersAsync(token);
        await RefreshDeadLettersAsync();
    }

    private void SwitchToMessaging()
    {
        if (DeadLetterActive && !TabSwitchBlocked) CloseDeadLetters();
    }

    private void CloseDeadLetters()
    {
        var session = deadLetterSession;
        deadLetterSession = null;
        deadLetterVersion++;
        deadLetterLoading = false;
        pendingPurge = focusedDeadLetter = null;
        deadLetterNextFocused = false;
        focusPurgeCancel = false;
        session?.Cancel();
        session?.Dispose();
    }

    private async Task NavigateHomeTabsAsync(KeyboardEventArgs args)
    {
        if (disposed || TabSwitchBlocked || args.Key is not ("ArrowLeft" or "ArrowRight" or "Home" or "End")) return;
        var open = args.Key == "End" || (args.Key != "Home" && !DeadLetterActive);
        if (open)
        {
            focusAfterRender = (deadLetterTab, false);
            await OpenDeadLettersAsync();
        }
        else
        {
            focusAfterRender = (messagingTab, false);
            SwitchToMessaging();
        }
    }

    private async Task PollDeadLettersAsync(CancellationToken token)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
        try
        {
            while (await timer.WaitForNextTickAsync(token))
            {
                await InvokeAsync(async () =>
                {
                    if (disposed || token.IsCancellationRequested || deadLetterLoading || deadLetterActing) return;
                    await RefreshDeadLettersAsync(background: true);
                    if (!disposed && !token.IsCancellationRequested) StateHasChanged();
                });
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (ObjectDisposedException) when (disposed) { }
    }

    private async Task SelectDeadLetterSubscriptionAsync(ChangeEventArgs args)
    {
        if (deadLetterActing || !DeadLetterActive || !int.TryParse(args.Value?.ToString(), out var index)
            || index < 0 || index >= deadLetterSubscriptions.Length) return;
        deadLetterSelection = deadLetterSubscriptions[index];
        deadLetterHistory.Clear();
        deadLetterCursor = null;
        deadLetterPage = null;
        deadLetterNextFocused = false;
        pendingPurge = focusedDeadLetter = null;
        deadLetterActionStatus = deadLetterActionError = null;
        await RefreshDeadLettersAsync();
    }

    private async Task MoveDeadLetterPageAsync(bool next)
    {
        if (deadLetterActing || !DeadLetterActive) return;
        if (next && deadLetterPage?.NextFromSequenceNumber is long cursor)
        {
            deadLetterHistory.Push(deadLetterCursor);
            deadLetterCursor = cursor;
        }
        else if (!next && deadLetterHistory.TryPop(out var previous))
        {
            deadLetterCursor = previous;
        }
        else return;

        deadLetterPage = null;
        deadLetterNextFocused = false;
        pendingPurge = focusedDeadLetter = null;
        // Either paging button can become disabled. Focus the results before the new page arrives.
        focusAfterRender = (deadLetterResult, false);
        await RefreshDeadLettersAsync();
    }

    private Task RefreshDeadLettersAsync() => RefreshDeadLettersAsync(background: false);

    private async Task RefreshDeadLettersAsync(bool background)
    {
        if (disposed || deadLetterActing || deadLetterSession is null) return;
        var token = deadLetterSession.Token;
        var version = ++deadLetterVersion;
        deadLetterLoading = true;
        if (!background) deadLetterLoadError = null;
        try
        {
            if (deadLetterSubscriptions.Length == 0)
            {
                var subscriptions = await ProducerApi.GetDeadLetterSubscriptionsAsync(token);
                if (!IsCurrent()) return;
                deadLetterSubscriptions = subscriptions;
                deadLetterSelection = subscriptions.FirstOrDefault();
            }
            if (deadLetterSelection is not { } pair) return;
            var page = await ProducerApi.PeekDeadLettersAsync(pair, deadLetterCursor, token);
            if (!IsCurrent()) return;
            // Only Next can lose availability on a refresh; Previous depends on navigation history.
            if (deadLetterNextFocused && deadLetterPage?.NextFromSequenceNumber is not null && page.NextFromSequenceNumber is null)
            {
                try
                {
                    // Move focus before rendering the native disabled attribute.
                    await deadLetterResult.FocusAsync(preventScroll: false);
                }
                catch (JSDisconnectedException)
                {
                    // The browser can disconnect while focus is moving.
                }
                if (!IsCurrent()) return;
                deadLetterNextFocused = false;
            }
            deadLetterPage = page;
            if (pendingPurge is { } purge && !IsVisible(purge.Pair, purge.Sequence))
                pendingPurge = null;
            if (focusedDeadLetter is { } focused && !IsVisible(focused.Pair, focused.Sequence))
            {
                focusedDeadLetter = null;
                focusAfterRender = (deadLetterResult, false);
            }
            deadLetterLoadError = null;
        }
        catch (Exception ex)
        {
            if (IsCurrent()) deadLetterLoadError = $"Could not refresh dead letters: {ex.Message}";
        }
        finally
        {
            if (IsCurrent()) deadLetterLoading = false;
        }

        bool IsCurrent() => !disposed && !token.IsCancellationRequested && version == deadLetterVersion;
    }

    private bool IsVisible(DeadLetterSubscription pair, long sequence) => pair == deadLetterSelection &&
        deadLetterPage?.Messages.Any(message => message.SequenceNumber == sequence) == true;

    private void BeginPurge(DeadLetterSubscription pair, long sequence)
    {
        if (disposed || !DeadLetterActive || deadLetterActing || !IsVisible(pair, sequence)) return;
        pendingPurge = (pair, sequence);
        focusPurgeCancel = true;
    }

    private void CancelPurge()
    {
        pendingPurge = null;
        focusAfterRender = (deadLetterResult, false);
    }

    private Task ConfirmPurgeAsync() => pendingPurge is { } target
        ? ActOnDeadLetterAsync(target.Pair, target.Sequence, resubmit: false) : Task.CompletedTask;

    private void DeadLetterBlurred(DeadLetterSubscription pair, long sequence)
    {
        if (focusedDeadLetter == (pair, sequence)) focusedDeadLetter = null;
    }

    private async Task ActOnDeadLetterAsync(DeadLetterSubscription pair, long sequence, bool resubmit)
    {
        if (disposed || !DeadLetterActive || deadLetterActing || !IsVisible(pair, sequence)) return;
        deadLetterVersion++;
        deadLetterLoading = false;
        deadLetterActing = true;
        deadLetterNextFocused = false;
        pendingPurge = focusedDeadLetter = null;
        deadLetterActionError = null;
        deadLetterActionStatus = resubmit ? "Resubmitting…" : "Purging…";
        // The initiating control may disappear. Keep focus on the outcome region throughout the request.
        focusAfterRender = (deadLetterResult, false);
        try
        {
            var result = resubmit
                ? await ProducerApi.ResubmitDeadLetterAsync(pair, sequence, pageLifetime.Token)
                : await ProducerApi.PurgeDeadLetterAsync(pair, sequence, pageLifetime.Token);
            if (disposed) return;
            deadLetterPage = null;
            deadLetterActionStatus = resubmit
                ? $"Published copy {result.PublishedMessageId} and completed original sequence {sequence}."
                : $"Purged original sequence {sequence}.";
        }
        catch (Exception ex)
        {
            if (disposed) return;
            deadLetterActionStatus = null;
            deadLetterActionError = ex is HttpRequestException { StatusCode: System.Net.HttpStatusCode.BadRequest or System.Net.HttpStatusCode.Conflict }
                ? ex.Message
                : $"Action could not be confirmed. {DeadLetterRecoveryGuidance} {ex.Message}";
        }
        finally
        {
            deadLetterActing = false;
        }
        if (!disposed)
        {
            // Render the action result even if the follow-up peek is slow.
            StateHasChanged();
            await RefreshDeadLettersAsync();
        }
    }
}
