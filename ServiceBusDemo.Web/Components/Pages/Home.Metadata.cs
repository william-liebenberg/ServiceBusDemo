using System.Collections.ObjectModel;
using Microsoft.AspNetCore.Components;
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
    private bool advancedOpen;
    private ElementReference advancedErrorSummary;
    private ElementReference addPropertyButton;

    private void AddProperty()
    {
        if (!Busy)
        {
            propertyRows.Add(new(++nextPropertyId));
            RevalidateMetadata();
        }
    }

    private void RemoveProperty(PropertyRow row)
    {
        var index = propertyRows.IndexOf(row);
        if (Busy || index < 0)
        {
            return;
        }

        propertyRows.RemoveAt(index);
        RevalidateMetadata();
        // Continue in the next row, or the previous row if the last one was removed.
        var target = propertyRows.Count == 0
            ? addPropertyButton
            : propertyRows[Math.Min(index, propertyRows.Count - 1)].KeyInput;
        focusAfterRender = (target, false);
    }

    private void RevalidateMetadata()
    {
        // Don't flag incomplete rows before the user has tried to send them.
        if (metadataErrors.Count > 0)
        {
            ValidateMetadata(out _);
        }
    }

    private void FocusMetadataErrors()
    {
        advancedOpen = true;
        focusAfterRender = (advancedErrorSummary, false);
    }

    private SendMessageRequest? SnapshotRequest(string message)
    {
        var properties = ValidateMetadata(out var optionalSubject);
        return metadataErrors.Count == 0
            ? new(message, optionalSubject, ApplicationProperties: properties.Count == 0 ? null : new ReadOnlyDictionary<string, string>(properties))
            : null;
    }

    private Dictionary<string, string> ValidateMetadata(out string? optionalSubject)
    {
        metadataErrors.Clear();
        optionalSubject = string.IsNullOrWhiteSpace(subject) ? null : subject;
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

        return properties;
    }

    private sealed class PropertyRow(int id)
    {
        public int Id { get; } = id;
        public string Key { get; set; } = "";
        public string Value { get; set; } = "";
        public ElementReference KeyInput { get; set; }
    }
}
