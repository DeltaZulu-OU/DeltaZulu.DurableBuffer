namespace DeltaZulu.DurableBuffer.Rx;

public readonly record struct RxCompletion(Exception? Exception)
{
    public bool IsSuccess => Exception is null;

    public bool IsFailure => Exception is not null;

    public static RxCompletion Success { get; } = new(null);

    public static RxCompletion Failure(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return new RxCompletion(exception);
    }
}
