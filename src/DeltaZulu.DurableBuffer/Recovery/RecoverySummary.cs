namespace DeltaZulu.DurableBuffer.Recovery;

public sealed record RecoverySummary
{
    public required int RecoveredChunks { get; init; }
    public required int QuarantinedFiles { get; init; }
    public required int DeadLetteredChunks { get; init; }
    public required long EstimatedLostRecords { get; init; }
}
