using System.Net.Http.Json;
using Bunit;
using Microsoft.AspNetCore.Components;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components.Pages;

namespace ServiceBusDemo.Tests;

public class HomeBurstAccessibilityTests
{
    [Fact]
    public async Task InvalidSharedMessageIsDescribedAndFocusedFromBurstForm()
    {
        using var client = new HttpClient(new AcceptMessage()) { BaseAddress = new Uri("http://producer.test") };
        await using var context = CreateContext(client, new DeliveryFeed());
        var home = context.Render<Home>();

        await home.Find("#burst-form").SubmitAsync();

        Assert.Single(home.FindAll("#message[aria-invalid=true][aria-describedby=message-error]"));
        Assert.Contains("Please enter a message", home.Find("#message-error").TextContent);
        var focus = Assert.Single(context.JSInterop.Invocations["Blazor._internal.domWrapper.focus"]);
        Assert.NotEmpty(Assert.IsType<ElementReference>(focus.Arguments[0]).Id);
        Assert.False(Assert.IsType<bool>(focus.Arguments[1]));
        Assert.Empty(home.FindAll("#burst-progress"));
    }

    [Fact]
    public async Task InvalidSharedOptionsAreDescribedAndFocusedInOrderAndClearOnEdit()
    {
        using var client = new HttpClient(new AcceptMessage()) { BaseAddress = new Uri("http://producer.test") };
        await using var context = CreateContext(client, new DeliveryFeed());
        var home = context.Render<Home>();
        home.Find("#message").Change("Order 42");
        home.Find("#fail-first").Change(true);
        home.Find("#slow-handler").Change(true);
        home.Find("#fail-first-count").Input("0");
        await home.Find("#burst-form").SubmitAsync();
        var failFirstFocus = LastFocus();
        Assert.Single(home.FindAll("#fail-first-count[aria-invalid=true][aria-describedby=fail-first-error]"));
        Assert.Contains("whole number", home.Find("#fail-first-error[aria-live=polite].text-danger-emphasis").TextContent);

        home.Find("#delay-seconds").Input("121");
        await home.Find("#burst-form").SubmitAsync();
        Assert.Equal(failFirstFocus, LastFocus());
        Assert.Single(home.FindAll("#delay-seconds[aria-invalid=true][aria-describedby=delay-seconds-error]"));
        Assert.Contains("whole number", home.Find("#delay-seconds-error[aria-live=polite].text-danger-emphasis").TextContent);

        home.Find("#fail-first-count").Input("2");
        Assert.Single(home.FindAll("#fail-first-count[aria-invalid=false]"));
        Assert.Empty(home.Find("#fail-first-error").TextContent);
        await home.Find("#burst-form").SubmitAsync();
        Assert.NotEqual(failFirstFocus, LastFocus());
        Assert.Equal(3, context.JSInterop.Invocations["Blazor._internal.domWrapper.focus"].Count);
        Assert.Empty(home.FindAll("#burst-progress"));

        home.Find("#slow-handler").Change(false);
        Assert.Empty(home.FindAll("#delay-seconds, #delay-seconds-error"));
        home.Find("#slow-handler").Change(true);
        Assert.Single(home.FindAll("#delay-seconds[aria-invalid=false]"));
        Assert.Empty(home.Find("#delay-seconds-error").TextContent);

        string LastFocus()
        {
            var focus = context.JSInterop.Invocations["Blazor._internal.domWrapper.focus"].Last();
            Assert.False(Assert.IsType<bool>(focus.Arguments[1]));
            var id = Assert.IsType<ElementReference>(focus.Arguments[0]).Id;
            Assert.NotEmpty(id);
            return id;
        }
    }

    [Fact]
    public async Task BurstLiveRegionSummarizesAcknowledgementsAndLateMatchingReports()
    {
        using var client = new HttpClient(new AcceptMessage()) { BaseAddress = new Uri("http://producer.test") };
        var feed = new DeliveryFeed();
        await using var context = CreateContext(client, feed);
        var home = context.Render<Home>();
        const string region = "#burst-counts-status[role=status][aria-live=polite][aria-atomic=true]";
        Assert.Empty(home.Find(region).TextContent);

        await home.Find("#message").ChangeAsync(new() { Value = "Order 42" });
        await home.Find("#burst-count").InputAsync(new() { Value = "1" });
        await home.Find("#burst-form").SubmitAsync();
        await home.WaitForAssertionAsync(() => Assert.Equal(
            "Burst: 1 of 1 sends acknowledged. Reported deliveries: Consumer One 0, Consumer Two 0.",
            home.Find(region).TextContent), TimeSpan.FromSeconds(5));

        feed.Add(Report("unrelated", "ConsumerTwo"));
        feed.Add(Report("acknowledged", "ConsumerOne"));
        feed.Add(Report("acknowledged", "ConsumerOne"));
        await home.WaitForAssertionAsync(() => Assert.Equal(
            "Burst: 1 of 1 sends acknowledged. Reported deliveries: Consumer One 1, Consumer Two 0.",
            home.Find(region).TextContent), TimeSpan.FromSeconds(5));
    }

    private static DeliveryEvent Report(string id, string consumer) =>
        new(id, nameof(MessagingPattern.CompetingConsumers), consumer, "topic", "subscription", "Order 42", 1, DateTimeOffset.UnixEpoch,
            DeliveryOutcomes.Completed, DateTimeOffset.UnixEpoch.AddMinutes(1), DateTimeOffset.UnixEpoch);

    private static BunitContext CreateContext(HttpClient client, DeliveryFeed feed)
    {
        var context = new BunitContext();
        context.JSInterop.SetupVoid("Blazor._internal.domWrapper.focus", _ => true).SetVoidResult();
        context.JSInterop.SetupVoid("Blazor._internal.NavigationLock.enableNavigationPrompt", _ => true).SetVoidResult();
        context.JSInterop.SetupVoid("Blazor._internal.NavigationLock.disableNavigationPrompt", _ => true).SetVoidResult();
        context.Services.AddSingleton(new ProducerApiClient(client));
        context.Services.AddControlTestClients();
        context.Services.AddSingleton(feed);
        return context;
    }

    private sealed class AcceptMessage : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.Accepted)
            {
                Content = JsonContent.Create(new SendMessageResult("acknowledged", "topic"))
            });
    }
}
