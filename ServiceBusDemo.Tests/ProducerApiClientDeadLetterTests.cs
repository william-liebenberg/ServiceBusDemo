using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Http;
using ServiceBusDemo.Web;

namespace ServiceBusDemo.Tests;

public class ProducerApiClientDeadLetterTests
{
    [Theory]
    [InlineData("resubmit")]
    [InlineData("purge")]
    public async Task WebActionClientDoesNotRetryTransientResponse(string action)
    {
        const string error = "Broker outcome could not be confirmed.";
        var requests = new List<(HttpMethod Method, Uri Uri)>();
        await using var factory = new WebApplicationFactory<DeliveryFeed>().WithWebHostBuilder(builder =>
        {
            builder.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(
                new Dictionary<string, string?> { ["Services:producer:http:0"] = "http://producer.test:8080" }));
            builder.ConfigureServices(services => services.PostConfigureAll<HttpClientFactoryOptions>(options =>
            {
                // Keep the actual Web handler pipeline; replace only the network transport.
                options.HttpMessageHandlerBuilderActions.Add(handler =>
                {
                    handler.PrimaryHandler.Dispose();
                    handler.PrimaryHandler = new StubTransport(requests, error);
                });
            }));
        });
        var producer = factory.Services.GetRequiredService<ProducerApiClient>();
        var pair = new DeadLetterSubscription("business-events", "business-events");
        var token = TestContext.Current.CancellationToken;

        var failure = await Assert.ThrowsAsync<HttpRequestException>(() => action == "resubmit"
            ? producer.ResubmitDeadLetterAsync(pair, 7, token)
            : producer.PurgeDeadLetterAsync(pair, 7, token));

        Assert.Equal(HttpStatusCode.BadGateway, failure.StatusCode);
        Assert.Contains(error, failure.Message);
        Assert.Equal((HttpMethod.Post, new Uri($"http://producer.test:8080/dead-letters/messages/7/{action}?topic=business-events&subscription=business-events")),
            Assert.Single(requests));
    }

    private sealed class StubTransport(List<(HttpMethod, Uri)> requests, string error) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            requests.Add((request.Method, request.RequestUri!));
            var response = new HttpResponseMessage(HttpStatusCode.BadGateway) { Content = new StringContent(error) };
            response.Headers.RetryAfter = new(TimeSpan.Zero);
            return Task.FromResult(response);
        }
    }
}
