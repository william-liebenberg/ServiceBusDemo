using ServiceBusDemo.ConsumerTwo;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.AddAzureServiceBusClient(connectionName: "messaging");
builder.Services.AddHostedService<MessageConsumer>();

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapGet("/", () => "ConsumerTwo is listening on business-events/business-events (competing consumers) and broadcast-events/consumer-two (pub/sub).");

app.MapDefaultEndpoints();

app.Run();
