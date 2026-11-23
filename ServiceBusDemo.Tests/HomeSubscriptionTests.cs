using System.Reflection;
using Bunit;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components.Pages;

namespace ServiceBusDemo.Tests;

public class HomeSubscriptionTests
{
    [Fact]
    public async Task DisposingHomeDetachesOnlyItsOwnFeedHandler()
    {
        using var client = new HttpClient();
        var feed = new DeliveryFeed();
        await using var firstContext = CreateContext();
        await using var secondContext = CreateContext();
        var first = firstContext.Render<Home>().Instance;
        var second = secondContext.Render<Home>().Instance;

        Assert.Collection(SubscriberTargets(feed), target => Assert.Same(first, target), target => Assert.Same(second, target));

        await firstContext.DisposeComponentsAsync();
        Assert.Same(second, Assert.Single(SubscriberTargets(feed)));

        await secondContext.DisposeComponentsAsync();
        Assert.Empty(SubscriberTargets(feed));

        BunitContext CreateContext()
        {
            var context = new BunitContext();
            context.Services.AddSingleton(new ProducerApiClient(client));
            context.Services.AddSingleton(feed);
            return context;
        }
    }

    private static IEnumerable<object?> SubscriberTargets(DeliveryFeed feed)
    {
        // Check the event itself: a leaked handler can still decline to render a disposed Home.
        var field = typeof(DeliveryFeed).GetField(nameof(DeliveryFeed.Changed), BindingFlags.Instance | BindingFlags.NonPublic)!;
        return (field.GetValue(feed) as Action)?.GetInvocationList().Select(handler => handler.Target) ?? [];
    }
}
