using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace ServiceBusDemo.ServiceDefaults;

public static class ConsumerControlEndpoints
{
    public static void MapConsumerControl<TConsumer>(this IEndpointRouteBuilder app)
        where TConsumer : ControlledMessageConsumer
    {
        var control = app.MapGroup("/control");
        control.MapGet("/status", (TConsumer consumer, HttpResponse response) =>
        {
            response.Headers.CacheControl = "no-store";
            return consumer.GetStatus();
        });
        control.MapPost("/pause", (TConsumer consumer, HttpRequest request) => ChangeAsync(request, consumer.PauseAsync));
        control.MapPost("/resume", (TConsumer consumer, HttpRequest request) => ChangeAsync(request, consumer.ResumeAsync));
    }

    private static async Task<IResult> ChangeAsync(HttpRequest request,
        Func<CancellationToken, Task<(bool Succeeded, ConsumerStatus Status)>> change)
    {
        // Blocks ordinary cross-site form submissions. This is not authentication.
        if (!request.Headers.TryGetValue(ConsumerControlProtocol.HeaderName, out var header)
            || header.Count != 1 || header[0] != ConsumerControlProtocol.HeaderValue)
        {
            return Results.BadRequest("The consumer control request header is required.");
        }

        var (succeeded, status) = await change(request.HttpContext.RequestAborted);
        return Results.Json(status, statusCode: succeeded ? StatusCodes.Status200OK : StatusCodes.Status503ServiceUnavailable);
    }
}
