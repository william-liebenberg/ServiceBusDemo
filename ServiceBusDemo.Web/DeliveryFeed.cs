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

    // One notification per Add, before Changed. Clear and retention do not affect it.
    // An already captured notification can still arrive after unsubscription.
    internal event Action<DeliveryEvent>? Reported;

    public void Add(DeliveryEvent delivery)
    {
        Action<DeliveryEvent>? reported;
        lock (_gate)
        {
            if (_deliveries.Count == Capacity)
            {
                _deliveries.Dequeue();
            }

            _deliveries.Enqueue(delivery);
            _reportCount++;
            reported = Reported;
        }

        reported?.Invoke(delivery);
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
