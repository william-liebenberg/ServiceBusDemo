using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Web;

public sealed class DeliveryFeed
{
    private const int Capacity = 200;
    private readonly object _gate = new();
    private readonly Queue<DeliveryEvent> _deliveries = new(Capacity);

    // Callbacks run outside the storage lock and may overlap on concurrent additions.
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
        }

        Changed?.Invoke();
    }

    public IReadOnlyList<DeliveryEvent> Snapshot()
    {
        lock (_gate)
        {
            return _deliveries.ToArray();
        }
    }
}
