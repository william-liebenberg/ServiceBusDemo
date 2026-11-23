using System.Text.Json;
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

app.MapPost("/api/deliveries", async (HttpRequest request, DeliveryFeed feed, CancellationToken cancellationToken) =>
{
    if (!request.HasJsonContentType())
    {
        return Results.StatusCode(StatusCodes.Status415UnsupportedMediaType);
    }

    DeliveryEvent? delivery;
    try
    {
        delivery = await request.ReadFromJsonAsync<DeliveryEvent>(cancellationToken);
    }
    catch (JsonException)
    {
        return Results.BadRequest();
    }

    if (delivery is null
        || delivery.Pattern is not ("CompetingConsumers" or "PubSub")
        || string.IsNullOrWhiteSpace(delivery.MessageId)
        || string.IsNullOrWhiteSpace(delivery.Consumer)
        || string.IsNullOrWhiteSpace(delivery.Topic)
        || string.IsNullOrWhiteSpace(delivery.Subscription)
        || delivery.Body is null
        || delivery.DeliveryCount < 1
        || delivery.ReceivedAt == default)
    {
        return Results.BadRequest();
    }

    feed.Add(delivery);
    return Results.Accepted();
}).Accepts<DeliveryEvent>("application/json");

app.MapDefaultEndpoints();

app.Run();
