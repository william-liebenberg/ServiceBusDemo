using System.Collections.ObjectModel;
using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Web.Components.Pages;

public partial class Home
{
    private string? subject;
    private string? priority;
    private string? region;
    private readonly List<PropertyRow> propertyRows = [];
    private int nextPropertyId;
    private readonly List<string> metadataErrors = [];

    private void AddProperty()
    {
        if (!Busy)
        {
            propertyRows.Add(new(++nextPropertyId));
        }
    }

    private void RemoveProperty(PropertyRow row)
    {
        if (!Busy)
        {
            propertyRows.Remove(row);
        }
    }

    private SendMessageRequest? SnapshotRequest(string message)
    {
        metadataErrors.Clear();
        var optionalSubject = string.IsNullOrWhiteSpace(subject) ? null : subject;
        if (optionalSubject is not null && !MessageMetadataRules.IsValid(optionalSubject, MessageMetadataRules.NameLimit))
        {
            metadataErrors.Add($"Subject must be at most {MessageMetadataRules.NameLimit} characters.");
        }

        var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(priority))
        {
            properties.Add("Priority", priority);
        }
        if (!string.IsNullOrWhiteSpace(region))
        {
            if (!MessageMetadataRules.IsValid(region, MessageMetadataRules.ValueLimit))
            {
                metadataErrors.Add($"Region must be at most {MessageMetadataRules.ValueLimit} characters.");
            }
            properties.Add("Region", region);
        }

        for (var i = 0; i < propertyRows.Count; i++)
        {
            var row = propertyRows[i];
            if (!MessageMetadataRules.IsValid(row.Key, MessageMetadataRules.NameLimit))
            {
                metadataErrors.Add($"Property {i + 1} key must be non-blank and at most {MessageMetadataRules.NameLimit} characters.");
            }
            else if (!properties.TryAdd(row.Key, row.Value))
            {
                metadataErrors.Add($"Property {i + 1} has a duplicate key: {row.Key}.");
            }

            if (!MessageMetadataRules.IsValid(row.Value, MessageMetadataRules.ValueLimit))
            {
                metadataErrors.Add($"Property {i + 1} value must be non-blank and at most {MessageMetadataRules.ValueLimit} characters.");
            }
        }

        return metadataErrors.Count == 0
            ? new(message, optionalSubject, ApplicationProperties: properties.Count == 0 ? null : new ReadOnlyDictionary<string, string>(properties))
            : null;
    }

    private sealed class PropertyRow(int id)
    {
        public int Id { get; } = id;
        public string Key { get; set; } = "";
        public string Value { get; set; } = "";
    }
}
