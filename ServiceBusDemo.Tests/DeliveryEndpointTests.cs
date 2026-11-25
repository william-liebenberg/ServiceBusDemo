using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Mvc.Testing;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;

namespace ServiceBusDemo.Tests;

public class DeliveryEndpointTests
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly DeliveryEvent Example = new(
        "message-1", "CompetingConsumers", "consumer-one", "business-events", "business-events", "Order placed", 1,
        new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.FromHours(2)));
    private static readonly string[] FieldNames =
        ["messageId", "pattern", "consumer", "topic", "subscription", "body", "deliveryCount", "receivedAt"];

    [Theory]
    [InlineData("CompetingConsumers", "Order placed", "application/json")]
    [InlineData("PubSub", "", "application/json; charset=utf-8")]
    [InlineData("PubSub", "Order placed", "application/json; charset=\"UTF-8\"")]
    public async Task ValidDeliveryIsAcceptedAndAddedToSingletonFeed(string pattern, string body, string contentType)
    {
        await using var factory = new WebApplicationFactory<DeliveryFeed>();
        using var client = CreateClient(factory);
        var feed = factory.Services.GetRequiredService<DeliveryFeed>();
        var notifications = 0;
        feed.Changed += () => notifications++;
        var delivery = Example with { Pattern = pattern, Body = body };

        using var content = new ByteArrayContent(JsonSerializer.SerializeToUtf8Bytes(delivery, JsonOptions));
        content.Headers.ContentType = MediaTypeHeaderValue.Parse(contentType);
        using var response = await client.PostAsync("/api/deliveries", content, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal(delivery, Assert.Single(feed.Snapshot()));
        Assert.Equal(1, notifications);
    }

    [Fact]
    public async Task WebRejectsUnexpectedHostBeforeHandlingDelivery()
    {
        await using var factory = new WebApplicationFactory<DeliveryFeed>();
        using var client = factory.CreateClient(new() { AllowAutoRedirect = false });
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/deliveries")
        {
            Content = JsonContent.Create(Example)
        };
        request.Headers.Host = "unexpected.example:5196";
        using var rejected = await client.SendAsync(request, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.BadRequest, rejected.StatusCode);
        Assert.Empty(factory.Services.GetRequiredService<DeliveryFeed>().Snapshot());
    }

    [Fact]
    public async Task UnsupportedCharsetDoesNotChangeFeedOrNotify()
    {
        await using var factory = new WebApplicationFactory<DeliveryFeed>();
        using var client = CreateClient(factory);
        var feed = factory.Services.GetRequiredService<DeliveryFeed>();
        feed.Add(Example);
        var notifications = 0;
        feed.Changed += () => notifications++;

        using var content = new StringContent(SerializeExample().ToJsonString(), Encoding.Unicode, "application/json");
        using var response = await client.PostAsync("/api/deliveries", content, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.UnsupportedMediaType, response.StatusCode);
        Assert.Equal(Example, Assert.Single(feed.Snapshot()));
        Assert.Equal(0, notifications);
    }

    [Theory]
    [MemberData(nameof(RejectedBodies))]
    public async Task RejectedDeliveryDoesNotChangeFeedOrNotify(string json)
    {
        await using var factory = new WebApplicationFactory<DeliveryFeed>();
        using var client = CreateClient(factory);
        var feed = factory.Services.GetRequiredService<DeliveryFeed>();
        feed.Add(Example);
        var notifications = 0;
        feed.Changed += () => notifications++;

        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        using var response = await client.PostAsync("/api/deliveries", content, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal(Example, Assert.Single(feed.Snapshot()));
        Assert.Equal(0, notifications);
    }

    public static TheoryData<string> RejectedBodies()
    {
        TheoryData<string> bodies = new() { "{", "", "null", "[]", "{}" };
        foreach (var field in FieldNames)
        {
            var missing = SerializeExample();
            missing.Remove(field);
            bodies.Add(missing.ToJsonString());
            bodies.Add(WithField(field, null));
        }

        foreach (var field in new[] { "messageId", "consumer", "topic", "subscription" })
        {
            bodies.Add(WithField(field, JsonValue.Create(" \t ")));
        }

        bodies.Add(WithField("pattern", JsonValue.Create("Other")));
        bodies.Add(WithField("body", JsonValue.Create(42)));
        bodies.Add(WithField("deliveryCount", JsonValue.Create(0)));
        bodies.Add(WithField("deliveryCount", JsonValue.Create("1")));
        bodies.Add(WithField("deliveryCount", JsonValue.Create(1.5)));
        bodies.Add(WithField("receivedAt", JsonValue.Create("not a date")));
        bodies.Add(WithField("receivedAt", JsonValue.Create("0001-01-01T00:00:00+00:00")));
        return bodies;
    }

    private static HttpClient CreateClient(WebApplicationFactory<DeliveryFeed> factory) => factory.CreateClient(new()
    {
        BaseAddress = new Uri("https://localhost"),
        AllowAutoRedirect = false
    });

    private static JsonObject SerializeExample() => JsonSerializer.SerializeToNode(Example, JsonOptions)!.AsObject();

    private static string WithField(string field, JsonNode? value)
    {
        var json = SerializeExample();
        json[field] = value;
        return json.ToJsonString();
    }
}
