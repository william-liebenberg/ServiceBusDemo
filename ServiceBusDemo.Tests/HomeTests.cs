using System.Net.Http.Json;
using AngleSharp.Html.Dom;
using Bunit;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components.Pages;

namespace ServiceBusDemo.Tests;

public class HomeTests
{
    private const string EnteredMessage = "  Order 42 was placed \t ";
    private const string TrimmedMessage = "Order 42 was placed";

    [Theory]
    [InlineData("#pattern-competing", "/sendMessage", "business-events")]
    [InlineData("#pattern-pubsub", "/broadcastMessage", "broadcast-events")]
    public async Task SuccessfulSendsRetainInputAndAddSeparateHistoryEntries(string patternSelector, string endpoint, string topic)
    {
        using var handler = new RecordingHandler(requestNumber => new HttpResponseMessage(HttpStatusCode.Accepted)
        {
            Content = JsonContent.Create(new SendMessageResult($"id-{requestNumber}", topic))
        });
        using var client = CreateClient(handler);
        await using var context = new BunitContext();
        context.Services.AddSingleton(new ProducerApiClient(client));
        var home = context.Render<Home>();

        var radio = home.Find(patternSelector);
        radio.Change(((IHtmlInputElement)radio).Value);
        home.Find("#message").Change(EnteredMessage);

        for (var send = 1; send <= 2; send++)
        {
            await home.Find("form").SubmitAsync();

            Assert.Equal(EnteredMessage, GetRenderedMessage(home));
            Assert.True(((IHtmlInputElement)home.Find(patternSelector)).IsChecked);
            Assert.Equal($"Message id-{send} published to '{topic}'.", home.Find(".alert-success[role=status]").TextContent);
        }

        Assert.Equal(new[] { (HttpMethod.Post, endpoint, TrimmedMessage), (HttpMethod.Post, endpoint, TrimmedMessage) }, handler.Requests);
        Assert.Equal(new[] { "id-2", "id-1" }, home.FindAll("tbody td:nth-child(4)").Select(cell => cell.TextContent));
        Assert.Equal(new[] { TrimmedMessage, TrimmedMessage }, home.FindAll("tbody td:last-child").Select(cell => cell.TextContent));
    }

    [Fact]
    public async Task FailedSendRetainsInputAndReenablesFormWithoutAddingHistory()
    {
        using var handler = new RecordingHandler(_ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        {
            Content = new StringContent("Try again later.")
        });
        using var client = CreateClient(handler);
        await using var context = new BunitContext();
        context.Services.AddSingleton(new ProducerApiClient(client));
        var home = context.Render<Home>();
        home.Find("#message").Change(EnteredMessage);

        await home.Find("form").SubmitAsync();

        Assert.Single(handler.Requests);
        Assert.Equal(EnteredMessage, GetRenderedMessage(home));
        Assert.Equal("Failed to send message: Producer API returned 503 (Service Unavailable): Try again later.",
            home.Find(".alert-danger[role=status]").TextContent);
        Assert.Empty(home.FindAll("tbody tr"));
        Assert.Empty(home.FindAll("fieldset[disabled], #message[disabled], button[type=submit][disabled]"));
    }

    [Theory]
    [InlineData(" \t ", 0, "Please enter a message.")]
    [InlineData("", 4097, "maximum length of 4096")]
    public async Task InvalidMessageRetainsInputAndDoesNotSend(string text, int extraLength, string validationMessage)
    {
        using var handler = new RecordingHandler(_ => throw new InvalidOperationException("Invalid input must not reach the producer."));
        using var client = CreateClient(handler);
        await using var context = new BunitContext();
        context.Services.AddSingleton(new ProducerApiClient(client));
        var home = context.Render<Home>();
        var message = text + new string('x', extraLength);
        home.Find("#message").Change(message);

        await home.Find("form").SubmitAsync();

        Assert.Empty(handler.Requests);
        Assert.Equal(message, GetRenderedMessage(home));
        Assert.Contains(validationMessage, home.Find(".validation-message").TextContent);
        Assert.Empty(home.FindAll("[role=status], tbody tr"));
    }

    // bUnit exposes the value from Blazor's render tree as an attribute rather than a DOM property.
    private static string? GetRenderedMessage(IRenderedComponent<Home> home) => home.Find("textarea#message").GetAttribute("value");

    private static HttpClient CreateClient(RecordingHandler handler) => new(handler, disposeHandler: false)
    {
        BaseAddress = new Uri("http://producer.test")
    };

    private sealed class RecordingHandler(Func<int, HttpResponseMessage> createResponse) : HttpMessageHandler
    {
        public List<(HttpMethod Method, string Path, string Message)> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = await request.Content!.ReadFromJsonAsync<SendMessageRequest>(cancellationToken);
            Requests.Add((request.Method, request.RequestUri!.AbsolutePath, body!.Message));
            return createResponse(Requests.Count);
        }
    }
}
