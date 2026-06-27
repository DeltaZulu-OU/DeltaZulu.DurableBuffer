namespace DeltaZulu.DurableBuffer.Dispatch;

public readonly record struct ChunkSendResult(ChunkSendStatus Status, string? Error = null);