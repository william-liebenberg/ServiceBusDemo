using ServiceBusDemo.ServiceDefaults;
using ServiceBusDemo.Web;

namespace ServiceBusDemo.Tests;

public class ConsumerControlClientTests
{
    [Fact]
    public async Task PauseCanWaitForAnInFlightSlowHandlerPastTheOldDeadline()
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        var services = new ServiceCollection();
        services.AddConsumerControlClient<ConsumerOneControlClient>("http://consumer-one.test");
        services.AddHttpClient(nameof(ConsumerOneControlClient))
            .ConfigurePrimaryHttpMessageHandler(() => new ControlTestClients.Handler(async (request, token) =>
            {
                Assert.Equal("/control/pause", request.RequestUri!.AbsolutePath);
                await Task.Delay(TimeSpan.FromSeconds(16), token);
                return ControlTestClients.Reply("ConsumerOne", ConsumerState.Paused);
            }));
        using var provider = services.BuildServiceProvider();

        var status = await provider.GetRequiredService<ConsumerOneControlClient>().PauseAsync(deadline.Token);

        Assert.Equal(ConsumerState.Paused, status.State);
    }
}
