namespace ServiceBusDemo.ServiceDefaults;

public static class MessageMetadataRules
{
    public const int NameLimit = 128;
    public const int ValueLimit = 1024;

    public static bool IsValid(string? value, int limit) =>
        !string.IsNullOrWhiteSpace(value) && value.Length <= limit;
}
