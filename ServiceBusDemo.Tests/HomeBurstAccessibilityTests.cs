using System.Net.Http.Json;
using Bunit;
using Microsoft.AspNetCore.Components;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components.Pages;

namespace ServiceBusDemo.Tests;

public class HomeBurstAccessibilityTests
{
    private const string FocusCall = "Blazor._internal.domWrapper.focus";

    [Fact]
    public async Task InvalidSharedMessageIsDescribedAndFocusedFromBurstForm()
    {
        using var client = new HttpClient(new AcceptMessage()) { BaseAddress = new Uri("http://producer.test") };
        await using var context = CreateContext(client, new DeliveryFeed());
        var home = context.Render<Home>();

        await home.Find("#burst-form").SubmitAsync();

        Assert.Single(home.FindAll("#message[aria-invalid=true][aria-describedby=message-error]"));
        Assert.Contains("Please enter a message", home.Find("#message-error").TextContent);
        Assert.Single(context.JSInterop.Invocations[FocusCall]);
        LastFocusTarget(context);
        Assert.Empty(home.FindAll("#burst-progress"));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task InvalidMetadataOpensAdvancedAndFocusesSummary(bool burst, bool previousBurstFailed)
    {
        var handler = new AcceptMessage(failOnRequest: previousBurstFailed ? 2 : null);
        using var client = new HttpClient(handler) { BaseAddress = new Uri("http://producer.test") };
        await using var context = CreateContext(client, new DeliveryFeed());
        var home = context.Render<Home>();
        await home.Find("#message").ChangeAsync(new() { Value = "Order 42" });
        string? previousBurstStatus = null;
        if (burst)
        {
            await home.Find("#burst-count").InputAsync(new() { Value = previousBurstFailed ? "2" : "1" });
            await home.Find("#burst-form").SubmitAsync();
            previousBurstStatus = home.Find(".burst-results #burst-status").TextContent;
            if (previousBurstFailed)
            {
                Assert.Contains("The producer may have accepted the last request despite the missing acknowledgement.", previousBurstStatus);
            }
            else
            {
                Assert.Equal("Burst complete.", previousBurstStatus);
            }
        }
        const string singleForm = "form:has(#message)";
        await home.Find(singleForm).SubmitAsync();
        Assert.Single(home.FindAll(".alert-success"));

        await home.Find("#advanced-heading").ClickAsync();
        await home.Find("#advanced-subject").InputAsync(new() { Value = new string('s', 129) });
        await home.Find("#advanced-heading").ClickAsync();
        Assert.Empty(home.FindAll("details[open]"));

        var requestCount = handler.RequestCount;
        var focusCalls = context.JSInterop.Invocations[FocusCall].Count;
        await home.Find(burst ? "#burst-form" : singleForm).SubmitAsync();

        var summary = home.Find("details[open] #advanced-errors[role=alert][tabindex='-1']");
        Assert.Contains("Subject must be at most 128 characters", summary.TextContent);
        Assert.Equal(focusCalls + 1, context.JSInterop.Invocations[FocusCall].Count);
        LastFocusTarget(context);
        Assert.Empty(home.FindAll(".alert-success"));
        Assert.Equal(requestCount, handler.RequestCount);
        if (burst)
        {
            Assert.Equal(previousBurstStatus, home.Find(".burst-results #burst-status").TextContent);
            Assert.Equal("1", home.Find("#burst-progress").GetAttribute("value"));
        }
    }

    [Fact]
    public async Task PropertyEditsRefreshErrorsAndRemovalRestoresFocus()
    {
        using var client = new HttpClient(new AcceptMessage()) { BaseAddress = new Uri("http://producer.test") };
        await using var context = CreateContext(client, new DeliveryFeed());
        var home = context.Render<Home>();
        await home.Find("#message").ChangeAsync(new() { Value = "Order 42" });
        await home.Find("#advanced-heading").ClickAsync();
        await home.Find("#advanced-priority").ChangeAsync(new() { Value = "High" });
        await home.Find("#add-property").ClickAsync();
        await home.Find("#property-key-1").InputAsync(new() { Value = "priority" });
        await home.Find("#add-property").ClickAsync();
        await home.Find("#property-key-2").InputAsync(new() { Value = "Tag" });
        Assert.Empty(home.Find("#advanced-errors").TextContent.Trim());
        await home.Find("#burst-form").SubmitAsync();
        Assert.Contains("Property 1 has a duplicate key", home.Find("#advanced-errors").TextContent);
        Assert.Contains("Property 2 value", home.Find("#advanced-errors").TextContent);
        var summary = LastFocusTarget(context);

        await home.Find("#advanced-priority").ChangeAsync(new() { Value = "" });
        Assert.DoesNotContain("duplicate key", home.Find("#advanced-errors").TextContent);
        await home.Find("button[aria-label='Remove property 1']").ClickAsync();
        var remainingRow = LastFocusTarget(context);
        Assert.NotEqual(summary, remainingRow);
        var error = Assert.Single(home.FindAll("#advanced-errors li"));
        Assert.Contains("Property 1 value", error.TextContent);
        Assert.Contains("Property 1 key", home.Find("label[for=property-key-2]").TextContent);

        await home.Find("#add-property").ClickAsync();
        Assert.Contains("Property 2 key", home.Find("#advanced-errors").TextContent);
        await home.Find("button[aria-label='Remove property 2']").ClickAsync();
        Assert.Equal(remainingRow, LastFocusTarget(context));
        Assert.DoesNotContain("Property 2", home.Find("#advanced-errors").TextContent);

        await home.Find("#property-value-2").InputAsync(new() { Value = "Corrected" });
        Assert.Empty(home.FindAll("#advanced-errors li"));
        await home.Find("button[aria-label='Remove property 1']").ClickAsync();
        var addButton = LastFocusTarget(context);
        Assert.NotEqual(remainingRow, addButton);
        Assert.NotEqual(summary, addButton);
        Assert.Equal(4, context.JSInterop.Invocations[FocusCall].Count);
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

    private static string LastFocusTarget(BunitContext context)
    {
        var focus = context.JSInterop.Invocations[FocusCall].Last();
        var reference = Assert.IsType<ElementReference>(focus.Arguments[0]);
        Assert.NotEmpty(reference.Id);
        Assert.False(Assert.IsType<bool>(focus.Arguments[1]));
        return reference.Id;
    }

    private static DeliveryEvent Report(string id, string consumer) =>
        new(id, nameof(MessagingPattern.CompetingConsumers), consumer, "topic", "subscription", "Order 42", 1, DateTimeOffset.UnixEpoch,
            DeliveryOutcomes.Completed, DateTimeOffset.UnixEpoch.AddMinutes(1), DateTimeOffset.UnixEpoch);

    private static BunitContext CreateContext(HttpClient client, DeliveryFeed feed)
    {
        var context = new BunitContext();
        context.JSInterop.SetupVoid(FocusCall, _ => true).SetVoidResult();
        context.JSInterop.SetupVoid("Blazor._internal.NavigationLock.enableNavigationPrompt", _ => true).SetVoidResult();
        context.JSInterop.SetupVoid("Blazor._internal.NavigationLock.disableNavigationPrompt", _ => true).SetVoidResult();
        context.Services.AddSingleton(new ProducerApiClient(client));
        context.Services.AddControlTestClients();
        context.Services.AddSingleton(feed);
        return context;
    }

    private sealed class AcceptMessage(int? failOnRequest = null) : HttpMessageHandler
    {
        public int RequestCount { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            RequestCount++;
            if (RequestCount == failOnRequest)
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
            }

            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Accepted)
            {
                Content = JsonContent.Create(new SendMessageResult("acknowledged", "topic"))
            });
        }
    }
}
