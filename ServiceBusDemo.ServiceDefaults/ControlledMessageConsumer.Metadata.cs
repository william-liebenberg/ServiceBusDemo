using System.Globalization;

namespace ServiceBusDemo.ServiceDefaults;

public abstract partial class ControlledMessageConsumer
{
    private static string FormatProperty(object? value)
    {
        try
        {
            return value switch
            {
                byte[] bytes => Convert.ToBase64String(bytes),
                _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? ""
            };
        }
        catch (Exception)
        {
            // Reporting must not fail message processing if an external value cannot be formatted.
            return "(unavailable)";
        }
    }
}
