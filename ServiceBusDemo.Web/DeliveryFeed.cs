using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Web;

public sealed class DeliveryFeed
{
    private const int Capacity = 200;
    private readonly object _gate = new();
    private readonly Queue<DeliveryEvent> _deliveries = new(Capacity);
    private long _reportCount;
    private long _clearCount;

    // Callbacks run outside the storage lock and may overlap on concurrent updates.
    public event Action? Changed;

    public void Add(DeliveryEvent delivery)
    {
        lock (_gate)
        {
            if (_deliveries.Count == Capacity)
            {
                _deliveries.Dequeue();
            }

            _deliveries.Enqueue(delivery);
            _reportCount++;
        }

        Changed?.Invoke();
    }

    public void Clear()
    {
        lock (_gate)
        {
            _deliveries.Clear();
            _clearCount++;
        }

        Changed?.Invoke();
    }

    public IReadOnlyList<DeliveryEvent> Snapshot() => Snapshot(out _, out _);

    // Keep counters and retained reports consistent even when update notifications overlap.
    internal IReadOnlyList<DeliveryEvent> Snapshot(out long reportCount, out long clearCount)
    {
        lock (_gate)
        {
            reportCount = _reportCount;
            clearCount = _clearCount;
            return _deliveries.ToArray();
        }
    }
}
