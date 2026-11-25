using Microsoft.AspNetCore.Components;
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
    private ElementReference failFirstCountInput;
    private ElementReference delaySecondsInput;

    private int? EmulatorMaxDeliveryCount => Services.GetService<IConfiguration>()
        ?.GetValue<int?>("ServiceBus:EmulatorMaxDeliveryCount");

    private SendMessageRequest? CreateSendRequest()
    {
        failFirstError = ValidateFailureOption(failFirst, failFirstCount, FailureLab.MaxFailFirstCount, out var count);
        delaySecondsError = ValidateFailureOption(slowHandler, delaySeconds, FailureLab.MaxDelaySeconds, out var seconds);
        return failFirstError is null && delaySecondsError is null && !string.IsNullOrWhiteSpace(model.Message)
            ? new(model.Message.Trim(), failFirst ? count : null, alwaysFail, slowHandler ? seconds : null)
            : null;
    }

    private void RevalidateFailFirst()
    {
        if (failFirstError is not null)
        {
            failFirstError = ValidateFailureOption(failFirst, failFirstCount, FailureLab.MaxFailFirstCount, out _);
        }
    }

    private void RevalidateDelay()
    {
        if (delaySecondsError is not null)
        {
            delaySecondsError = ValidateFailureOption(slowHandler, delaySeconds, FailureLab.MaxDelaySeconds, out _);
        }
    }

    private static string? ValidateFailureOption(bool enabled, string input, int max, out int value)
    {
        value = 0;
        return !enabled || TryBoundedInteger(input, max, out value)
            ? null
            : $"Enter a whole number from {FailureLab.Minimum} to {max}.";
    }

    private void FocusInvalidFailureOption()
    {
        if (failFirstError is not null)
        {
            focusAfterRender = (failFirstCountInput, false);
        }
        else if (delaySecondsError is not null)
        {
            focusAfterRender = (delaySecondsInput, false);
        }
    }

    private static string LocalTimeLabel(DateTimeOffset time) => $"{time:G} (UTC{time:zzz})";
}
