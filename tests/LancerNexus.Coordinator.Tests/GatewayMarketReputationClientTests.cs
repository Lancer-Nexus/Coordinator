using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class GatewayMarketReputationClientTests
{
    [Fact]
    public async Task ReadsReputationOverDedicatedHttpsServiceCredential()
    {
        var request = Request();
        using var handler = new CaptureHandler(request);
        using var http = new HttpClient(handler);
        var key = new string('k', 40);
        var configuration = Configuration("https://gateway.example/", key);
        var client = new GatewayMarketReputationClient(http, configuration);

        var result = await client.ReadAsync(request);

        Assert.Equal(MarketFactionReputationStatusV1.Found, result.Status);
        Assert.True(result.IsValidFor(request));
        Assert.Equal("Bearer", handler.Authorization?.Scheme);
        Assert.Equal(key, handler.Authorization?.Parameter);
        Assert.Equal("https://gateway.example/internal/v1/market/reputation/read", handler.RequestUri?.ToString());
        Assert.Equal(request.ExpectedReputationRevision, handler.ForwardedRequest?.ExpectedReputationRevision);
    }

    [Fact]
    public async Task FailsClosedWhenGatewayResponseDoesNotMatchCorrelation()
    {
        var request = Request();
        using var handler = new CaptureHandler(request, invalidCorrelation: true);
        using var http = new HttpClient(handler);
        var client = new GatewayMarketReputationClient(http, Configuration("https://gateway.example/", new string('k', 40)));

        var result = await client.ReadAsync(request);

        Assert.Equal(MarketFactionReputationStatusV1.Unavailable, result.Status);
        Assert.True(result.IsValidFor(request));
    }

    [Fact]
    public async Task DoesNotSendWhenGatewayCredentialIsNotConfigured()
    {
        var request = Request();
        using var handler = new CaptureHandler(request);
        using var http = new HttpClient(handler);
        var client = new GatewayMarketReputationClient(http, Configuration("http://gateway.example/", new string('k', 40)));

        var result = await client.ReadAsync(request);

        Assert.Equal(MarketFactionReputationStatusV1.Unavailable, result.Status);
        Assert.Null(handler.RequestUri);
    }

    private static MarketFactionReputationReadRequestV1 Request() => new()
    {
        CorrelationId = Guid.NewGuid(),
        CharacterId = 77,
        ExpectedReputationRevision = 4
    };

    private static IConfiguration Configuration(string endpoint, string key) =>
        new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Coordinator:GatewayMarketBaseUrl"] = endpoint,
            ["Coordinator:GatewayMarketApiKey"] = key
        }).Build();

    private sealed class CaptureHandler(
        MarketFactionReputationReadRequestV1 expectedRequest,
        bool invalidCorrelation = false) : HttpMessageHandler
    {
        public AuthenticationHeaderValue? Authorization { get; private set; }
        public Uri? RequestUri { get; private set; }
        public MarketFactionReputationReadRequestV1? ForwardedRequest { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Authorization = request.Headers.Authorization;
            RequestUri = request.RequestUri;
            var body = await request.Content!.ReadAsStringAsync(cancellationToken);
            ForwardedRequest = JsonSerializer.Deserialize<MarketFactionReputationReadRequestV1>(body,
                new JsonSerializerOptions(JsonSerializerDefaults.Web));
            var response = new MarketFactionReputationReadResponseV1
            {
                CorrelationId = invalidCorrelation ? Guid.NewGuid() : expectedRequest.CorrelationId,
                CharacterId = expectedRequest.CharacterId,
                Status = MarketFactionReputationStatusV1.Found,
                ReputationRevision = 4,
                Factions = [new MarketFactionReputationV1 { FactionId = "li_n_grp", Value = 0.75f }]
            };
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = JsonContent.Create(response) };
        }
    }
}
