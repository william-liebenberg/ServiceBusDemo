using System.Text.Json;
using Microsoft.AspNetCore.Http.Json;
using Microsoft.Extensions.Options;
using Microsoft.Net.Http.Headers;
using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;
using ServiceBusDemo.Web.Components;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults & Aspire client integrations.
builder.AddServiceDefaults();

// Add services to the container.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddOutputCache();
builder.Services.AddSingleton<DeliveryFeed>();

builder.Services.AddHttpClient<ProducerApiClient>(client =>
    {
        // This URL uses "https+http://" to indicate HTTPS is preferred over HTTP.
        // Learn more about service discovery scheme resolution at https://aka.ms/dotnet/sdschemes.
        client.BaseAddress = new("https+http://producer");
    });

builder.Services.AddConsumerControlClient<ConsumerOneControlClient>("https+http://consumer-one");
builder.Services.AddConsumerControlClient<ConsumerTwoControlClient>("https+http://consumer-two");

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();

app.UseAntiforgery();

app.UseOutputCache();

app.MapStaticAssets();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.MapPost("/api/deliveries", async (HttpRequest request, DeliveryFeed feed, IOptions<JsonOptions> jsonOptions, CancellationToken cancellationToken) =>
{
    if (!request.HasJsonContentType()
        || !MediaTypeHeaderValue.TryParse(request.ContentType, out var contentType)
        || (contentType.Charset.HasValue
            && !HeaderUtilities.RemoveQuotes(contentType.Charset).Equals("utf-8", StringComparison.OrdinalIgnoreCase)))
    {
        return Results.StatusCode(StatusCodes.Status415UnsupportedMediaType);
    }

    DeliveryEvent? delivery;
    try
    {
        // Only UTF-8 is supported, so no charset lookup or transcoding is needed.
        delivery = await JsonSerializer.DeserializeAsync<DeliveryEvent>(
            request.Body, jsonOptions.Value.SerializerOptions, cancellationToken);
    }
    catch (JsonException)
    {
        return Results.BadRequest();
    }

    if (delivery is null
        || delivery.Pattern is not ("CompetingConsumers" or "PubSub" or "TopicFilters")
        || string.IsNullOrWhiteSpace(delivery.MessageId)
        || string.IsNullOrWhiteSpace(delivery.Consumer)
        || string.IsNullOrWhiteSpace(delivery.Topic)
        || string.IsNullOrWhiteSpace(delivery.Subscription)
        || delivery.Body is null
        || delivery.DeliveryCount < 1
        || delivery.ReceivedAt == default
        || delivery.Outcome is not (DeliveryOutcomes.Completed or DeliveryOutcomes.Abandoned or DeliveryOutcomes.DeadLettered)
        || delivery.InitialLockedUntil == default
        || delivery.SettledAt == default
        || delivery.SettledAt < delivery.ReceivedAt)
    {
        return Results.BadRequest();
    }

    feed.Add(delivery);
    return Results.Accepted();
}).Accepts<DeliveryEvent>("application/json");

app.MapDefaultEndpoints();

app.Run();
