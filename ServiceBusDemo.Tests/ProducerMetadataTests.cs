using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using ServiceBusDemo.Web;

namespace ServiceBusDemo.Tests;

public class ProducerMetadataTests
{
    private const string BusinessEvents = "business-events";
    private const string BroadcastEvents = "broadcast-events";
    private const string RoutedEvents = "routed-events";

    [Theory]
    [InlineData("/sendMessage", BusinessEvents)]
    [InlineData("/broadcastMessage", BroadcastEvents)]
    public async Task BothRoutesMapMetadataAndRejectInvalidMetadataWithoutSending(string endpoint, string topic)
    {
        var token = TestContext.Current.CancellationToken;
        var competing = new RecordingSender(BusinessEvents);
        var broadcast = new RecordingSender(BroadcastEvents);
        await using var factory = new WebApplicationFactory<ProducerEndpointAssembly>().WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Production");
            builder.ConfigureTestServices(services =>
            {
                services.AddKeyedSingleton<ServiceBusSender>(BusinessEvents, competing);
                services.AddKeyedSingleton<ServiceBusSender>(BroadcastEvents, broadcast);
            });
        });
        using var client = factory.CreateClient(new() { BaseAddress = new Uri("https://localhost") });
        var sender = topic == competing.EntityPath ? competing : broadcast;
        var other = sender == competing ? broadcast : competing;

        foreach (var json in new[]
        {
            """{"message":"legacy"}""",
            """{"message":"legacy","subject":null,"correlationId":null,"applicationProperties":null}""",
            """{"message":"legacy","applicationProperties":{}}"""
        })
        {
            using var response = await PostAsync(json);
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            var sent = sender.Messages.Last();
            Assert.Null(sent.Subject);
            Assert.Null(sent.CorrelationId);
            Assert.Empty(sent.ApplicationProperties);
        }

        var subject = $" {new string('s', 126)} ";
        var correlationId = new string('c', 128);
        var key = $" {new string('k', 126)} ";
        var value = $" {new string('v', 1022)} ";
        using (var response = await PostAsync(JsonSerializer.Serialize(new
        {
            message = "Order", subject, correlationId,
            applicationProperties = new Dictionary<string, string> { [key] = value, ["Priority"] = "High" }
        })))
        {
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
            var sent = sender.Messages.Last();
            var result = await response.Content.ReadFromJsonAsync<SendMessageResult>(token);
            Assert.Equal(new SendMessageResult(sent.MessageId, topic), result);
            Assert.True(Guid.TryParse(sent.MessageId, out _));
            Assert.Equal("text/plain", sent.ContentType);
            Assert.Equal("Order", sent.Body.ToString());
            Assert.Equal(subject, sent.Subject);
            Assert.Equal(correlationId, sent.CorrelationId);
            Assert.Equal(value, Assert.IsType<string>(sent.ApplicationProperties[key]));
            Assert.Equal("High", Assert.IsType<string>(sent.ApplicationProperties["Priority"]));
        }

        foreach (var metadata in new[]
        {
            "\"subject\":\" \"",
            "\"correlationId\":\"\"",
            $"\"subject\":\"{new string('s', 129)}\"",
            $"\"correlationId\":\"{new string('c', 129)}\"",
            "\"applicationProperties\":{\"\":\"value\"}",
            "\"applicationProperties\":{\"  \":\"value\"}",
            "\"applicationProperties\":{\"key\":null}",
            "\"applicationProperties\":{\"key\":\"  \"}",
            "\"applicationProperties\":{\"key\":9}",
            $"\"applicationProperties\":{{\"{new string('k', 129)}\":\"value\"}}",
            $"\"applicationProperties\":{{\"key\":\"{new string('v', 1025)}\"}}"
        })
        {
            using var response = await PostAsync("{\"message\":\"Order\"," + metadata + "}");
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            Assert.Equal(4, sender.Messages.Count);
            Assert.Empty(other.Messages);
        }

        async Task<HttpResponseMessage> PostAsync(string json)
        {
            using var body = new StringContent(json, Encoding.UTF8, "application/json");
            return await client.PostAsync(endpoint, body, token);
        }
    }

    [Fact]
    public async Task RoutedMessagesCombineMetadataAndRoutingFields()
    {
        var sender = new RecordingSender(RoutedEvents);
        await using var factory = new WebApplicationFactory<ProducerEndpointAssembly>().WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Production");
            builder.ConfigureTestServices(services => services.AddKeyedSingleton<ServiceBusSender>(RoutedEvents, sender));
        });
        using var client = factory.CreateClient(new() { BaseAddress = new Uri("https://localhost") });
        using var response = await client.PostAsJsonAsync("/routedMessage", new
        {
            message = "Order", subject = "orders", correlationId = "42",
            priority = "High", region = "EU",
            applicationProperties = new Dictionary<string, string> { ["Priority"] = "Low", ["Tag"] = "example" }
        }, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var message = Assert.Single(sender.Messages);
        Assert.Equal("orders", message.Subject);
        Assert.Equal("42", message.CorrelationId);
        Assert.Equal("High", message.ApplicationProperties["Priority"]);
        Assert.Equal("EU", message.ApplicationProperties["Region"]);
        Assert.Equal("example", message.ApplicationProperties["Tag"]);
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
