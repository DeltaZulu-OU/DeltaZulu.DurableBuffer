using System.Text;
using System.Text.Json;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class JsonRecordSerializerTests
{
    private sealed record TestRecord(string Name, int Value);

    [TestMethod]
    public void Serialize_ProducesValidUtf8Json()
    {
        var serializer = new JsonRecordSerializer<TestRecord>();
        var record = new TestRecord("test", 42);

        var bytes = serializer.Serialize(record);
        var json = Encoding.UTF8.GetString(bytes.Span);
        var parsed = JsonDocument.Parse(json);

        Assert.AreEqual("test", parsed.RootElement.GetProperty("Name").GetString());
        Assert.AreEqual(42, parsed.RootElement.GetProperty("Value").GetInt32());
    }

    [TestMethod]
    public void Serialize_WithOptions_RespectsNamingPolicy()
    {
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        var serializer = new JsonRecordSerializer<TestRecord>(options);
        var record = new TestRecord("hello", 1);

        var bytes = serializer.Serialize(record);
        var json = Encoding.UTF8.GetString(bytes.Span);

        Assert.Contains("\"name\"", json);
        Assert.Contains("\"value\"", json);
    }

    [TestMethod]
    public void Serialize_Dictionary_ProducesValidJson()
    {
        var serializer = new JsonRecordSerializer<Dictionary<string, object>>();
        var record = new Dictionary<string, object> { ["key"] = "value" };

        var bytes = serializer.Serialize(record);
        var json = Encoding.UTF8.GetString(bytes.Span);
        var parsed = JsonDocument.Parse(json);

        Assert.AreEqual("value", parsed.RootElement.GetProperty("key").GetString());
    }
}
