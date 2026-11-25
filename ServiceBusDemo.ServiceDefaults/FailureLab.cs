namespace ServiceBusDemo.ServiceDefaults;

public static class FailureLab
{
    public const string FailFirstCountProperty = "fail-first-count";
    public const string AlwaysFailProperty = "always-fail";
    public const string DelaySecondsProperty = "delay-seconds";
    public const int Minimum = 1;
    public const int MaxFailFirstCount = 10;
    public const int MaxDelaySeconds = 120;
    public const int AttemptLimit = 5;
    public const string DeadLetterReason = "FailureLabAttemptLimit";

    public static bool IsReservedProperty(string key) =>
        string.Equals(key, FailFirstCountProperty, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(key, AlwaysFailProperty, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(key, DelaySecondsProperty, StringComparison.OrdinalIgnoreCase);
}
