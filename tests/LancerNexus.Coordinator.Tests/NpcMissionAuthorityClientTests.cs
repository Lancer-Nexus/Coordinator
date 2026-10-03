using System.Net;
using System.Net.Http.Json;
using LancerNexus.Protocol;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class NpcMissionAuthorityClientTests
{
    [Theory]
    [InlineData(0, true)]
    [InlineData(1, false)]
    [InlineData(2, false)]
    [InlineData(3, false)]
    [InlineData(4, false)]
    public async Task AuthorityRequiresBoundSuccessfulReply(int scenario, bool expected)
    {
        var request = new NpcMissionAuthorityRequestV1
        {
            TransferId = Guid.NewGuid(), SourceInstanceId = "source", TargetInstanceId = "target",
            TargetSystemId = "li02", Decision = NpcTransferState.Committed
        };
        var reply = new NpcMissionAuthorityResultV1
        {
            TransferId = scenario == 1 ? Guid.NewGuid() : request.TransferId, SourceInstanceId = "source",
            TargetInstanceId = "target", TargetSystemId = "li02", Decision = request.Decision,
            Accepted = scenario != 2, CommittedLeaseVersion = 15
        };
        using var http = new HttpClient(new Handler(scenario, reply));
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Coordinator:NpcMissionAuthorityBaseUrl"] = "https://gateway.invalid/",
            ["Coordinator:NpcMissionAuthorityApiKey"] = new string('x', 32)
        }).Build();
        var result = await new NpcMissionAuthorityClient(http, config).AuthorizeAsync(request);
        Assert.Equal(expected, result.Authorized);
    }

    private sealed class Handler(int scenario, NpcMissionAuthorityResultV1 reply) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Assert.Equal("https://gateway.invalid/internal/v1/npc-mission-authority", request.RequestUri!.ToString());
            Assert.Equal("Bearer", request.Headers.Authorization!.Scheme);
            return Task.FromResult(new HttpResponseMessage(scenario == 3 ? HttpStatusCode.ServiceUnavailable : HttpStatusCode.OK)
            {
                Content = scenario == 4 ? new StringContent("invalid-json") : JsonContent.Create(reply)
            });
        }
    }
}
