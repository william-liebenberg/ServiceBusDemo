using ServiceBusDemo.ConsumerOne;
using ServiceBusDemo.ServiceDefaults;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.AddAzureServiceBusClient(connectionName: "messaging");
builder.Services.AddDeliveryReporting();
builder.Services.AddSingleton<MessageConsumer>();
builder.Services.AddHostedService(services => services.GetRequiredService<MessageConsumer>());

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapGet("/", () => "ConsumerOne is listening on business-events/business-events (competing consumers) and broadcast-events/consumer-one (pub/sub).");
app.MapConsumerControl<MessageConsumer>();
app.MapDefaultEndpoints();

app.Run();
