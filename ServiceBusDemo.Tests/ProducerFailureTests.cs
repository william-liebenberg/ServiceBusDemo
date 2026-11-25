using System.Net.Http.Json;
using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using ServiceBusDemo.Producer;
using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Tests;

public class ProducerFailureTests
{
    [Theory]
    [InlineData("/sendMessage", "business-events")]
    [InlineData("/broadcastMessage", "broadcast-events")]
    public async Task RoutesValidateAndMapFailureOptions(string route, string topic)
    {
        var token = TestContext.Current.CancellationToken;
        var sender = new RecordingSender(topic);
        var other = new RecordingSender("other");
        await using var factory = new WebApplicationFactory<ProducerApiMarker>().WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Testing");
            builder.UseSetting("ConnectionStrings:messaging", "Endpoint=sb://localhost/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==");
            builder.ConfigureServices(services =>
            {
                services.AddKeyedSingleton<ServiceBusSender>("business-events", topic == "business-events" ? sender : other);
                services.AddKeyedSingleton<ServiceBusSender>("broadcast-events", topic == "broadcast-events" ? sender : other);
            });
        });
        using var client = factory.CreateClient(new() { BaseAddress = new Uri("https://localhost") });
        using var normal = await client.PostAsJsonAsync(route, new { message = "Order" }, token);
        Assert.Equal(HttpStatusCode.Accepted, normal.StatusCode);
        Assert.Empty(Assert.Single(sender.Messages).ApplicationProperties);
        using var configured = await client.PostAsJsonAsync(route, new { message = "Order", failFirstCount = 10, alwaysFail = true, delaySeconds = 120 }, token);
        Assert.Equal(HttpStatusCode.Accepted, configured.StatusCode);
        var properties = sender.Messages[1].ApplicationProperties;
        Assert.Equal(3, properties.Count);
        Assert.Equal(10, properties[FailureLab.FailFirstCountProperty]);
        Assert.Equal(true, properties[FailureLab.AlwaysFailProperty]);
        Assert.Equal(120, properties[FailureLab.DelaySecondsProperty]);

        foreach (var option in new[] { "\"failFirstCount\":0", "\"failFirstCount\":11", "\"failFirstCount\":1.5", "\"failFirstCount\":\"2\"",
                     "\"delaySeconds\":0", "\"delaySeconds\":121", "\"delaySeconds\":1.5", "\"alwaysFail\":\"true\"" })
        {
            using var content = new StringContent($$"""{"message":"Order",{{option}}}""", Encoding.UTF8, "application/json");
            using var rejected = await client.PostAsync(route, content, token);
            Assert.Equal(HttpStatusCode.BadRequest, rejected.StatusCode);
        }
        Assert.Equal(2, sender.Messages.Count);
        Assert.Empty(other.Messages);
    }

    private sealed class RecordingSender(string topic) : ServiceBusSender
    {
        public override string EntityPath => topic;
        public List<ServiceBusMessage> Messages { get; } = [];

        public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
        {
            Messages.Add(message);
            return Task.CompletedTask;
        }
    }
}
