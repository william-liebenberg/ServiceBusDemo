using ServiceBusDemo.ServiceDefaults;

namespace ServiceBusDemo.Web;

internal sealed class BurstDeliveryTracker : IDisposable
{
    internal const int EarlyIdCapacity = 256;
    internal const string ConsumerOneId = "ConsumerOne";
    internal const string ConsumerTwoId = "ConsumerTwo";
    private const int ConsumerOneBit = 1;
    private const int ConsumerTwoBit = 2;

    private readonly DeliveryFeed _feed;
    private readonly string _patternName;
    private readonly object _gate = new();
    private readonly Dictionary<string, int> _acknowledged = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _early = new(StringComparer.Ordinal);
    private bool _inFlight;
    private bool _overflowed;
    private bool _disposed;
    private int _sent;
    private int _consumerOne;
    private int _consumerTwo;

    public BurstDeliveryTracker(DeliveryFeed feed, MessagingPattern pattern, int requested)
    {
        _feed = feed;
        Pattern = pattern;
        Requested = requested;
        _patternName = pattern.ToString();
        _feed.Reported += Observe;
    }

    public MessagingPattern Pattern { get; }
    public int Requested { get; }

    public void BeginRequest()
    {
        lock (_gate)
        {
            _inFlight = true;
        }
    }

    public void EndRequest(string? acknowledgedId = null)
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            if (acknowledgedId is not null)
            {
                _sent++;
                _acknowledged.TryAdd(acknowledgedId, 0);
                if (_early.TryGetValue(acknowledgedId, out var recipients))
                {
                    Count(acknowledgedId, recipients);
                }
            }

            _inFlight = false;
            _early.Clear();
        }
    }

    public (int Sent, int ConsumerOne, int ConsumerTwo, bool Overflowed) Snapshot()
    {
        lock (_gate)
        {
            return (_sent, _consumerOne, _consumerTwo, _overflowed);
        }
    }

    private void Observe(DeliveryEvent delivery)
    {
        if (delivery.Pattern != _patternName)
        {
            return;
        }

        var recipient = delivery.Consumer switch
        {
            ConsumerOneId => ConsumerOneBit,
            ConsumerTwoId => ConsumerTwoBit,
            _ => 0
        };
        if (recipient == 0)
        {
            return;
        }

        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            var id = delivery.MessageId;
            if (_acknowledged.ContainsKey(id))
            {
                Count(id, recipient);
            }
            else if (_inFlight)
            {
                if (_early.TryGetValue(id, out var existing))
                {
                    _early[id] = existing | recipient;
                }
                else if (_early.Count < EarlyIdCapacity)
                {
                    _early.Add(id, recipient);
                }
                else
                {
                    _overflowed = true;
                }
            }
        }
    }

    private void Count(string id, int recipients)
    {
        var existing = _acknowledged[id];
        var added = recipients & ~existing;
        _acknowledged[id] = existing | recipients;
        _consumerOne += (added & ConsumerOneBit) != 0 ? 1 : 0;
        _consumerTwo += (added & ConsumerTwoBit) != 0 ? 1 : 0;
    }

    public void Dispose()
    {
        _feed.Reported -= Observe;
        lock (_gate)
        {
            _disposed = true;
            _early.Clear();
        }
    }
}
