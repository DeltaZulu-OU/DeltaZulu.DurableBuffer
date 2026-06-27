namespace DeltaZulu.DurableBuffer.Recovery;

public interface IRecoveryManager
{
    ValueTask<RecoverySummary> RecoverAsync(CancellationToken cancellationToken = default);
}