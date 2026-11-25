using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Web.Components.Pages;

public partial class Home
{
    private bool failFirst;
    private bool alwaysFail;
    private bool slowHandler;
    private string failFirstCount = "2";
    private string delaySeconds = "5";
    private string? failFirstError;
    private string? delaySecondsError;

    private int? EmulatorMaxDeliveryCount => Services.GetService<IConfiguration>()
        ?.GetValue<int?>("ServiceBus:EmulatorMaxDeliveryCount");

    private SendMessageRequest? CreateSendRequest()
    {
        var count = 0;
        var seconds = 0;
        var countValid = !failFirst || TryBoundedInteger(failFirstCount, FailureLab.MaxFailFirstCount, out count);
        var delayValid = !slowHandler || TryBoundedInteger(delaySeconds, FailureLab.MaxDelaySeconds, out seconds);
        failFirstError = countValid ? null : $"Enter a whole number from {FailureLab.Minimum} to {FailureLab.MaxFailFirstCount}.";
        delaySecondsError = delayValid ? null : $"Enter a whole number from {FailureLab.Minimum} to {FailureLab.MaxDelaySeconds}.";
        return countValid && delayValid && !string.IsNullOrWhiteSpace(model.Message)
            ? new(model.Message.Trim(), failFirst ? count : null, alwaysFail, slowHandler ? seconds : null)
            : null;
    }

    private static string LocalTimeLabel(DateTimeOffset time) => $"{time:G} (UTC{time:zzz})";
}
