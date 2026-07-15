namespace DeltaZulu.DurableBuffer.Rx;

public interface IRxDispatchDiagnostics<T>
{
    IReadOnlyList<RxDispatchSnapshot<T>> GetDispatchSnapshots();

    IReadOnlyList<RxSubscriptionSnapshot> GetSubscriptionSnapshots();
}

public sealed record RxSubscriptionSnapshot(
    long RxSubscriptionId,
    string SubscriberType,
    DateTimeOffset SubscribedUtc,
    long RequestedTotal,
    long DeliveredTotal,
    DateTimeOffset? CancelledUtc,
    DateTimeOffset? CompletedUtc,
    DateTimeOffset LastSignalUtc,
    long CurrentDemand,
    long InFlightItems);

public sealed record RxDispatchSnapshot<T>(
    string ChunkId,
    long ChunkSequence,
    DateTimeOffset DispatchedUtc,
    string? ConsumerId,
    int AttemptCount,
    string LastOutcome,
    T Item);
