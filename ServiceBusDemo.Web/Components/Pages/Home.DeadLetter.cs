using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;

namespace ServiceBusDemo.Web.Components.Pages;

public partial class Home
{
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
    private ElementReference messagingTab;
    private ElementReference deadLetterTab;
    private bool DeadLetterActive => deadLetterSession is not null;

    private async Task OpenDeadLettersAsync()
    {
        if (disposed || DeadLetterActive) return;
        deadLetterSession = CancellationTokenSource.CreateLinkedTokenSource(pageLifetime.Token);
        var token = deadLetterSession.Token;
        _ = PollDeadLettersAsync(token);
        await RefreshDeadLettersAsync();
    }

    private void CloseDeadLetters()
    {
        var session = deadLetterSession;
        deadLetterSession = null;
        deadLetterVersion++;
        deadLetterLoading = false;
        session?.Cancel();
        session?.Dispose();
    }

    private async Task NavigateHomeTabsAsync(KeyboardEventArgs args)
    {
        if (args.Key is not ("ArrowLeft" or "ArrowRight" or "Home" or "End")) return;
        var open = args.Key == "End" || (args.Key != "Home" && !DeadLetterActive);
        if (open)
        {
            await deadLetterTab.FocusAsync();
            await OpenDeadLettersAsync();
        }
        else
        {
            await messagingTab.FocusAsync();
            CloseDeadLetters();
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
            deadLetterPage = page;
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

    private async Task ActOnDeadLetterAsync(DeadLetterSubscription pair, long sequence, bool resubmit)
    {
        if (disposed || !DeadLetterActive || deadLetterActing || pair != deadLetterSelection
            || deadLetterPage?.Messages.Any(message => message.SequenceNumber == sequence) != true) return;
        deadLetterVersion++;
        deadLetterLoading = false;
        deadLetterActing = true;
        deadLetterActionError = null;
        deadLetterActionStatus = resubmit ? "Resubmitting…" : "Purging…";
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
                : $"Action could not be confirmed. Refresh before retrying.{(resubmit ? " Retrying a resubmit can produce duplicates." : "")} {ex.Message}";
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
