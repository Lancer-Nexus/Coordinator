using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

public interface IGatewayMarketReputationClient
{
    Task<MarketFactionReputationReadResponseV1> ReadAsync(
        MarketFactionReputationReadRequestV1 request,
        CancellationToken cancellationToken = default);
}

public sealed class GatewayMarketReputationClient(HttpClient http, IConfiguration configuration)
    : IGatewayMarketReputationClient
{
    public async Task<MarketFactionReputationReadResponseV1> ReadAsync(
        MarketFactionReputationReadRequestV1 request,
        CancellationToken cancellationToken = default)
    {
        if (!request.IsValid())
            return Unavailable(request);
        var endpointValue = configuration["Coordinator:GatewayMarketBaseUrl"];
        var key = configuration["Coordinator:GatewayMarketApiKey"];
        if (!Uri.TryCreate(endpointValue, UriKind.Absolute, out var endpoint) || endpoint.Scheme != Uri.UriSchemeHttps ||
            !string.IsNullOrEmpty(endpoint.UserInfo) || !string.IsNullOrEmpty(endpoint.Query) ||
            !string.IsNullOrEmpty(endpoint.Fragment) || string.IsNullOrWhiteSpace(key) ||
            Encoding.UTF8.GetByteCount(key) < 32)
            return Unavailable(request);

        using var message = new HttpRequestMessage(HttpMethod.Post,
            new Uri(endpoint, "/internal/v1/market/reputation/read"));
        message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", key);
        message.Content = JsonContent.Create(request);
        var timeoutSeconds = configuration.GetValue<int?>("Coordinator:GatewayMarketTimeoutSeconds") ?? 5;
        if (timeoutSeconds <= 0)
            return Unavailable(request);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));
        try
        {
            using var response = await http.SendAsync(message, timeout.Token);
            if (response.StatusCode is not (HttpStatusCode.OK or HttpStatusCode.Conflict or HttpStatusCode.NotFound))
                return Unavailable(request);
            var result = await response.Content.ReadFromJsonAsync<MarketFactionReputationReadResponseV1>(timeout.Token);
            return result is not null && result.IsValidFor(request) ? result : Unavailable(request);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return Unavailable(request);
        }
        catch (Exception error) when (error is HttpRequestException or JsonException or NotSupportedException)
        {
            return Unavailable(request);
        }
    }

    private static MarketFactionReputationReadResponseV1 Unavailable(
        MarketFactionReputationReadRequestV1 request) => new()
    {
        CorrelationId = request.CorrelationId,
        CharacterId = request.CharacterId,
        Status = MarketFactionReputationStatusV1.Unavailable
    };
}
