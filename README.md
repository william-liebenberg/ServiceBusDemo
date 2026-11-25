# Azure Service Bus Topics — Patterns in Practice

<p align="center">
  <strong>A small, runnable .NET Aspire app for seeing what Azure Service Bus topics actually do.</strong>
  <br />
  Compare <em>competing consumers</em>, <em>publish/subscribe</em>, and <em>topic filters</em>, send messages from a web UI, and follow deliveries — no Azure subscription required for the local demo.
</p>

<p align="center">
  <img alt=".NET 11" src="https://img.shields.io/badge/.NET-11.0-512BD4?logo=dotnet&logoColor=white" />
  <img alt="Azure Service Bus" src="https://img.shields.io/badge/Azure-Service%20Bus-0078D4?logo=microsoftazure&logoColor=white" />
  <img alt=".NET Aspire" src="https://img.shields.io/badge/orchestrated%20with-.NET%20Aspire-512BD4?logoColor=white" />
</p>

## What you'll learn

- How a **topic** fans messages out to its **subscriptions**.
- Why consumers sharing one subscription compete for each message.
- How independent subscriptions give each consumer its own copy.
- How broker-side subscription rules route messages based on application properties.
- How to publish with `ServiceBusSender` and receive with `ServiceBusProcessor`.
- How .NET Aspire wires the producer, consumers, and local Service Bus emulator together.

## The app at a glance

```mermaid
flowchart LR
    User([Developer]) --> UI[Web UI]
    UI -->|HTTP| API[Producer API]
    API -->|sendMessage| SharedTopic[(business-events)]
    API -->|broadcastMessage| FanoutTopic[(broadcast-events)]
    API -->|routedMessage| RoutedTopic[(routed-events)]

    SharedTopic --> SharedSub[[business-events subscription]]
    SharedSub -. "one delivery per message" .-> C1[Consumer One]
    SharedSub -. "or one delivery per message" .-> C2[Consumer Two]

    FanoutTopic --> SubOne[[consumer-one subscription]]
    FanoutTopic --> SubTwo[[consumer-two subscription]]
    SubOne --> C1
    SubTwo --> C2
    RoutedTopic --> HighSub[[high-priority]]
    RoutedTopic --> EuSub[[eu-orders]]
    RoutedTopic --> AllSub[[all]]
    HighSub --> C1
    AllSub --> C1
    EuSub --> C2

    classDef app fill:#0b5cab,color:#fff,stroke:#073b73,stroke-width:2px;
    classDef topic fill:#6f42c1,color:#fff,stroke:#4c2889,stroke-width:2px;
    classDef subscription fill:#008272,color:#fff,stroke:#005a50,stroke-width:2px;
    class UI,API,C1,C2 app;
    class SharedTopic,FanoutTopic,RoutedTopic topic;
    class SharedSub,SubOne,SubTwo,HighSub,EuSub,AllSub subscription;
```

The Web UI calls the Producer API. The API publishes to one of three topics; each topic's subscriptions determine which consumers receive the message. Aspire creates the local resources and supplies connection configuration to the services.

## Pattern 1: Competing consumers

**One topic → one shared subscription → multiple consumers.** Consumer One and Consumer Two both receive from `business-events/business-events`. For each message, Service Bus delivers it to **one** receiver on that subscription, not a copy to both. This lets you scale out workers that perform the same kind of work.

```mermaid
flowchart LR
    P[Producer] -->|publish| T[(business-events topic)]
    T --> S[[business-events subscription]]
    S -. "message A → one receiver" .-> C1[Consumer One]
    S -. "or one receiver" .-> C2[Consumer Two]

    classDef app fill:#0b5cab,color:#fff,stroke:#073b73,stroke-width:2px;
    classDef topic fill:#6f42c1,color:#fff,stroke:#4c2889,stroke-width:2px;
    classDef subscription fill:#008272,color:#fff,stroke:#005a50,stroke-width:2px;
    class P,C1,C2 app;
    class T topic;
    class S subscription;
```

Both consumers attach to the same subscription name in [AppHost.cs](ServiceBusDemo.AppHost/AppHost.cs):

```csharp
var beTopic = asb.AddServiceBusTopic("businessEvent", topicName: "business-events");
var beSub = beTopic.AddServiceBusSubscription("business-events");

builder.AddProject<Projects.ServiceBusDemo_ConsumerOne>("consumer-one")
    .WithReference(asb)
    .WithEnvironment("ServiceBus__TopicName", beTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__SubscriptionName", beSub.Resource.SubscriptionName);

builder.AddProject<Projects.ServiceBusDemo_ConsumerTwo>("consumer-two")
    .WithReference(asb)
    .WithEnvironment("ServiceBus__TopicName", beTopic.Resource.TopicName)
    .WithEnvironment("ServiceBus__SubscriptionName", beSub.Resource.SubscriptionName);
```

The producer's `/sendMessage` endpoint uses the sender registered for that topic. It assigns a message ID and sends the message:

```csharp
var message = new ServiceBusMessage(request.Message)
{
    MessageId = Guid.NewGuid().ToString(),
    ContentType = "text/plain"
};

await sender.SendMessageAsync(message, cancellationToken);
```

**Good fit:** distribute independent jobs across equivalent workers. Competing consumers are not a broadcast mechanism; if both services need every event, use separate subscriptions instead.

## Pattern 2: Publish/subscribe (fan-out)

**One topic → one subscription per consumer.** `broadcast-events` has a `consumer-one` subscription and a `consumer-two` subscription. Service Bus places a copy of each published message in each subscription, so both consumers can process the same event independently and at their own pace.

```mermaid
flowchart LR
    P[Producer] -->|publish once| T[(broadcast-events topic)]
    T --> S1[[consumer-one subscription]]
    T --> S2[[consumer-two subscription]]
    S1 -->|its copy| C1[Consumer One]
    S2 -->|its copy| C2[Consumer Two]

    classDef app fill:#0b5cab,color:#fff,stroke:#073b73,stroke-width:2px;
    classDef topic fill:#6f42c1,color:#fff,stroke:#4c2889,stroke-width:2px;
    classDef subscription fill:#008272,color:#fff,stroke:#005a50,stroke-width:2px;
    class P,C1,C2 app;
    class T topic;
    class S1,S2 subscription;
```

Aspire declares a distinct subscription for each consumer. No subscription filters are configured in this demo, so both subscriptions receive every message sent to this topic:

```csharp
var broadcastTopic = asb.AddServiceBusTopic("broadcastEvent", topicName: "broadcast-events");
var consumerOneSub = broadcastTopic.AddServiceBusSubscription(
    "broadcast-consumer-one", subscriptionName: "consumer-one");
var consumerTwoSub = broadcastTopic.AddServiceBusSubscription(
    "broadcast-consumer-two", subscriptionName: "consumer-two");
```

The Producer API exposes this route as `/broadcastMessage`. Each consumer starts a processor for its own broadcast subscription as well as the shared competing-consumer subscription:

```csharp
var processor = client.CreateProcessor(topic, subscription, new ServiceBusProcessorOptions
{
    AutoCompleteMessages = true,
    MaxConcurrentCalls = 1
});

processor.ProcessMessageAsync += args =>
{
    logger.LogInformation("[{Pattern}] {Consumer} received {MessageId} from {Topic}/{Subscription}",
        pattern, ConsumerName, args.Message.MessageId, topic, subscription);
    return Task.CompletedTask;
};

await processor.StartProcessingAsync(stoppingToken);
```

**Good fit:** independent downstream systems that each need the event, such as notifications, analytics, and auditing.

## Pattern 3: Topic filters

The broker evaluates each subscription independently on `routed-events`:

| Subscription | Rule | Consumer |
| --- | --- | --- |
| `high-priority` | `Priority = 'High'` | Consumer One |
| `eu-orders` | `Region = 'EU'` | Consumer Two |
| `all` | Unfiltered | Consumer One |

AppHost uses Aspire 13.6.5's subscription `Rules` API to declare one correlation rule per filtered subscription. Aspire serializes the rules into the emulator's startup configuration. Each filtered subscription starts with its configured rule only, not a default match-all rule. `all` has no custom rules and receives every message. The producer waits for the routed subscriptions; no separate setup command or consumer-side filtering is needed.

In the UI, select **Topic filters** and enter a message and optional **Priority** and **Region** values. Try `High` alone, `EU` alone, both together, and neither. Both matching values route to all three subscriptions; neither (including nonmatching values) routes only to `all`. Consumer One can show two cards with the same message ID because it owns two distinct routed subscriptions.

The sent history lists reported subscriptions by message ID, using only retained events. Clearing the feed or its capacity limit can remove these reports without changing sent history. Missing reports do not prove non-delivery. **Topic filters** sends one message at a time; the burst sender is disabled for this pattern.

## Run it locally

### Prerequisites

- [.NET 11 SDK](https://dotnet.microsoft.com/download)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running (Aspire runs the Service Bus emulator in a container)

### Start the app

From the repository root:

```powershell
dotnet run --project ServiceBusDemo.AppHost
```

Aspire starts the emulator with all three topics and their subscriptions, and launches the web UI, producer, and two consumers. Open the **webfrontend** endpoint shown in the Aspire dashboard, choose a pattern, enter a message, and select **Send**. The dashboard is also where you can inspect resource health and the consumer logs.

To observe the difference, send one message with each pattern and look at the consumer logs:

- **Competing consumers:** find `[CompetingConsumers]`; only one of the two consumers handles a given message in the normal case.
- **Publish/subscribe:** find `[PubSub]`; both consumers log the same message ID, each from its own subscription.
- **Topic filters:** find `[TopicFilters]`; the same message ID appears once for each matching subscription.

### Add message metadata

- **Competing consumers** and **Publish/subscribe**: In **Advanced**, select an optional **Priority** (`Low`, `Normal`, or `High`) and enter an optional **Region**. These settings apply to single sends and bursts. A burst captures the Advanced settings once and uses them for every message.
- **Topic filters**: Enter optional **Priority** and **Region** values under **Routing properties**, not **Advanced**. Use these fields rather than custom rows named `Priority` or `Region`. Burst sending is disabled; send one message at a time.

For any pattern, use **Advanced** to set an optional **Subject** or add and remove custom key/value rows. Blank Subject and Region and a blank or unselected Priority field are omitted. The dedicated fields send `Priority` and `Region` as application properties. Custom rows require both a key and a value; keys cannot duplicate another row or a populated Advanced field for Priority or Region, ignoring case.

**Sent this session** shows the metadata captured for each acknowledged send; delivery cards show the metadata the consumers actually received from the broker.

### Try a burst

Choose **Competing consumers** or **Publish/subscribe** and enter a message. Set **Count** (1–500, default 10), leave **Limit rate** unchecked to go as fast as possible, or enable it and enter a whole-number rate (1–1000 messages per second). Select **Send burst** and watch the acknowledged-send progress and the two consumer bars. With all reports arriving once, the competing-consumer bars sum to the acknowledged send count, while each pub/sub bar reaches that count.

The Web UI loops sequentially over the existing producer routes rather than using a batch API. This keeps the per-message acknowledgements and lets the UI pace request starts and cancel between requests. A fixed rate is a cap, not a throughput guarantee. **Cancel burst** interrupts any pacing delay and stops new requests; it lets an already-started request finish and counts its acknowledgement if it succeeds. A failed request stops the burst without retrying: the producer may have accepted it even if the UI didn't receive confirmation.

Use **Cancel burst** and wait for it to finish before leaving if you want to see the final acknowledged count. Leaving or reloading during an active burst can interrupt it and loses the page's progress and statistics. The producer may still accept an in-flight message without the page receiving confirmation. The page shows this warning during the burst; reloads and tab closes use the browser's standard leave-page prompt when allowed.

The bars count **reported deliveries**, not broker backlog. They follow acknowledged IDs from the latest burst in this page session, including reports that arrive after sending stops. **Clear** removes shared delivery cards, not burst stats or sent history. Reports are best-effort and can be lost, and the UI warns if its bounded early-report buffer fills. Repeat reports for the same message and consumer count once; redelivery to a different competing consumer can put the total above expected.

### Pause and resume consumers

Each delivery lane has **Pause** and **Resume** buttons that control all processors for that consumer without stopping its process. Pause waits for in-flight processing to finish. Resume starts the same processors again. The badges refresh automatically, including after changes from another tab. A consumer process restart starts all its processors normally; pause is not persisted. **Mixed** means processors are in different states; retry Pause or Resume to bring them back to the same state.

1. Pause **Consumer Two** and wait for **Paused**. Send a competing-consumer message or burst. The new deliveries should appear in Consumer One's lane.
2. Pause **Consumer One** too. Send more competing-consumer messages and note their message IDs. Resume Consumer One and watch for those IDs in its lane as it processes the backlog.
3. With Consumer Two still paused, send a publish/subscribe message. Consumer One receives its own copy. Resume Consumer Two to see the other copy with the same message ID.

Pausing does not remove pending messages. Normal broker expiry still applies, and delivery reports are best-effort: a missing card does not prove a message wasn't processed. The badges report processor state, not broker backlog.

Consumer control endpoints are internal in Aspire. The Web app uses `GET /control/status` and sends `X-ServiceBusDemo-Control: 1` on `POST /control/pause` and `POST /control/resume`. This header blocks ordinary cross-site form submissions; it is **not authentication**, and cross-origin access is not enabled. Do not expose this unauthenticated demo, **including its Web UI**, to untrusted users or networks.

The Web frontend and both consumers accept only loopback hostnames by default (`localhost`, `127.0.0.1`, `[::1]`). For a different trusted setup, set `AllowedHosts` per app to an explicit semicolon-separated list of the hostnames it needs to accept, without ports or a wildcard. Host filtering does not authenticate callers.

### Subscription depth in the emulator

**Result: the management call responds, but does not provide usable active or dead-letter counts through the SDK.**
Microsoft's [emulator overview and limitations](https://learn.microsoft.com/en-us/azure/service-bus-messaging/overview-emulator) describe .NET entity management, and the [local testing guide](https://learn.microsoft.com/en-us/azure/service-bus-messaging/test-locally-with-service-bus-emulator) specifies the separate HTTP management port (5300 by default). That does not guarantee that subscription runtime counts decode correctly.

The AppHost launched with `dotnet run --project ServiceBusDemo.AppHost` selected `mcr.microsoft.com/azure-messaging/servicebus-emulator:2.0.0`. To keep it running for the probe, the same AppHost was started detached with `aspire start --apphost ServiceBusDemo.AppHost --no-build -lp https`. Docker was remote in this environment: host-loopback checks timed out or refused connections, so the probe shared the emulator container's network namespace and used `Endpoint=sb://127.0.0.1:5300;...;UseDevelopmentEmulator=true` for management and port 5672 for AMQP.

With Azure.Messaging.ServiceBus 7.20.2, `ServiceBusAdministrationClient.GetSubscriptionRuntimePropertiesAsync` returned HTTP 200 on all three configured subscriptions. After sending with no receivers, completing shared messages and dead-lettering a broadcast copy, the SDK still reported `ActiveMessageCount=0` and `DeadLetterMessageCount=0` throughout. AMQP peek independently found two active messages and one dead-letter on `broadcast-events/consumer-one`, and three active messages on `broadcast-events/consumer-two`. The HTTP bodies had `MessageCount=3` and count details with nonstandard XML names (for example, `iqV` and `iqv`), not the named SDK count fields. This is not a startup failure or evidence of an empty subscription. `SubscriptionRuntimeProperties` also has no `ScheduledMessageCount` property; even with a future-dated send in the probe there was no named scheduled count available from this call.

Home therefore shows **Estimated pending** and **Dead-letter: unavailable** for each subscription. It counts distinct acknowledged message IDs for the row's topic that have no matching report for that topic and subscription in the current delivery feed. Either consumer can satisfy the shared subscription's report; broadcast subscriptions are counted independently. There is no broker polling for these estimates.

The estimate covers only sends acknowledged on this page, not external sends or sends in other tabs. Reports are best-effort, precede broker completion, and the feed retains only the most recent 200 reports. Clearing the feed or evicting an older report can increase the estimate; reloading loses this page's sent history.

To walk through the estimate, start with a fresh Home page:

1. Pause both consumers and wait for both badges to show **Paused**. Send a competing-consumer burst of two. Only the shared row should rise, to two.
2. Resume Consumer One. Watch the shared row return to zero as its reports arrive.
3. With Consumer Two still paused, send one publish/subscribe message. The `consumer-one` copy should quickly return to zero after its report; `consumer-two` should stay at one until you resume Consumer Two and its report arrives.

Verified separately in Chromium against the running AppHost and emulator: the shared estimate rose from 0 to 2 with both consumers paused and returned to 0 after resuming Consumer One. A broadcast then left Consumer Two's estimate at 1 while Consumer One's was 0; resuming Consumer Two returned all three rows to 0. The UI sequence is also covered in bUnit with simulated control and delivery responses.

### Run the tests

```powershell
dotnet test ServiceBusDemo.slnx
```

## Try the producer API directly

The Producer API has three POST routes. Use the producer endpoint URL shown in the Aspire dashboard in place of `<producer-url>`:

```bash
curl -X POST "<producer-url>/sendMessage" \
  -H "Content-Type: application/json" \
  -d '{"message":"Order 42 was placed"}'

curl -X POST "<producer-url>/broadcastMessage" \
  -H "Content-Type: application/json" \
  -d '{"message":"Order 42 was placed"}'

curl -X POST "<producer-url>/routedMessage" \
  -H "Content-Type: application/json" \
  -d '{"message":"Priority order","priority":"High"}'

curl -X POST "<producer-url>/routedMessage" \
  -H "Content-Type: application/json" \
  -d '{"message":"EU order","region":"EU"}'

curl -X POST "<producer-url>/routedMessage" \
  -H "Content-Type: application/json" \
  -d '{"message":"Priority EU order","priority":"High","region":"EU"}'

curl -X POST "<producer-url>/routedMessage" \
  -H "Content-Type: application/json" \
  -d '{"message":"Standard order"}'
```

A routed request maps nonempty string `priority` and `region` values to the broker application properties `Priority` and `Region` respectively.
A successful request returns `202 Accepted` with the generated `messageId` and topic name. The API rejects an empty message with `400 Bad Request`.

Both routes also accept optional `subject`, `correlationId`, and a string-valued `applicationProperties` object. Existing message-only requests still work, and each optional field can be omitted or set to `null`. For example:

```json
{
  "message": "Order 42 was placed",
  "subject": "Order placed",
  "correlationId": "order-42",
  "applicationProperties": { "Priority": "High", "Region": "West" }
}
```

Subject, CorrelationId, and application-property keys are limited to 128 characters; property values are limited to 1024. Supplied strings must contain a non-whitespace character, and property values cannot be null. Valid text is preserved without trimming. Invalid metadata returns `400 Bad Request` without sending. CorrelationId is set through the API, not the UI.

## Delivery event ingest

`POST /api/deliveries` on **webfrontend** accepts a JSON [`DeliveryEvent`](ServiceBusDemo.ServiceDefaults/DeliveryEvent.cs) and returns `202 Accepted` for a valid event. After logging a delivery, each consumer queues a report that includes the broker's delivery count. Each consumer can queue up to 256 reports and sends them in the background with a two-second timeout and no retries. If Web is unavailable or the reporting queue is full, reports may be lost without affecting Service Bus processing. Pending reports are discarded at shutdown.

Reports may include the broker's `subject`, `correlationId`, and `applicationProperties`. Application-property values from other producers are formatted as strings for display; older reports can omit all metadata.

The feed keeps the most recent 200 events in memory and loses them when the Web app restarts. The endpoint is unauthenticated and intended only for the local demo. Do not expose it publicly: any caller can submit events, and the 200-event cap limits the number of entries, not the size of each event.

## Project map

| Project | Role |
| --- | --- |
| [`ServiceBusDemo.AppHost`](ServiceBusDemo.AppHost/AppHost.cs) | Defines the emulator, topics, subscriptions, and service wiring. |
| [`ServiceBusDemo.Web`](ServiceBusDemo.Web/Components/Pages/Home.razor) | Interactive UI for selecting a pattern and publishing a message. |
| [`ServiceBusDemo.Producer`](ServiceBusDemo.Producer/Program.cs) | HTTP API with a `ServiceBusSender` for each topic. |
| [`ServiceBusDemo.ConsumerOne`](ServiceBusDemo.ConsumerOne/MessageConsumer.cs) | Worker listening to shared, broadcast, `high-priority`, and `all` subscriptions. |
| [`ServiceBusDemo.ConsumerTwo`](ServiceBusDemo.ConsumerTwo/MessageConsumer.cs) | Worker listening to shared, broadcast, and `eu-orders` subscriptions. |
| [`ServiceBusDemo.Tests`](ServiceBusDemo.Tests/WebTests.cs) | Aspire integration test for all patterns and reporting during Web outages. |

## A few Service Bus details worth keeping in mind

- A **topic** is the publish endpoint; a **subscription** is an independently consumable view of messages published to that topic.
- A subscription is a durable messaging entity, not just a listener. Its messages remain available to that subscription until they are completed, expired, or otherwise removed under the entity's rules.
- Competing consumers share the subscription's work. Separate subscriptions each get their own copy. To add a new subscriber, create a new subscription.
- Delivery is generally **at least once**. A process failure or lock expiry can result in redelivery, so real handlers should be safe to run more than once. This demo logs messages and auto-completes them; it is illustrating routing, not implementing business processing or retries.
- This project uses Aspire's **local Service Bus emulator** for development. For Azure, provision the entities and configure the client to use an Azure namespace and an appropriate identity/credential; don't treat the local emulator as a production broker.

## Extend the demo

1. Add a subscription in `ServiceBusDemo.AppHost/AppHost.cs`.
2. Pass its topic and subscription names to the intended consumer with `WithEnvironment`.
3. Add a processor for that entity in the consumer's `MessageConsumer`.
4. Publish a message and verify the resulting logs in the Aspire dashboard.

If this helped you understand the patterns, a ⭐ on the repo is always appreciated!
