using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

public sealed record NpcMissionAuthorityCheck(bool Authorized, string ReasonCode);
public interface INpcMissionAuthorityClient
{
    Task<NpcMissionAuthorityCheck> AuthorizeAsync(NpcMissionAuthorityRequestV1 request,
        CancellationToken cancellationToken = default);
}

public sealed class NpcMissionAuthorityClient(HttpClient http, IConfiguration configuration) : INpcMissionAuthorityClient
{
    public async Task<NpcMissionAuthorityCheck> AuthorizeAsync(NpcMissionAuthorityRequestV1 request,
        CancellationToken cancellationToken = default)
    {
        var url = configuration["Coordinator:NpcMissionAuthorityBaseUrl"];
        var key = configuration["Coordinator:NpcMissionAuthorityApiKey"];
        if (!request.IsValid() || !Uri.TryCreate(url, UriKind.Absolute, out var endpoint) || endpoint.Scheme != "https" ||
            !string.IsNullOrEmpty(endpoint.UserInfo) || !string.IsNullOrEmpty(endpoint.Query) ||
            !string.IsNullOrEmpty(endpoint.Fragment) || string.IsNullOrWhiteSpace(key) || Encoding.UTF8.GetByteCount(key) < 32)
            return new(false, "mission_authority_not_configured");
        using var message = new HttpRequestMessage(HttpMethod.Post,
            new Uri(endpoint, "/internal/v1/npc-mission-authority"));
        message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", key);
        message.Content = JsonContent.Create(request);
        try
        {
            using var response = await http.SendAsync(message, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return new(false, "mission_authority_unavailable");
            var result = await response.Content.ReadFromJsonAsync<NpcMissionAuthorityResultV1>(cancellationToken);
            return result is not null && result.Authorizes(request)
                ? new(true, "mission_authority_confirmed")
                : new(false, "mission_authority_rejected");
        }
        catch (Exception exception) when (exception is HttpRequestException or JsonException or NotSupportedException ||
            exception is OperationCanceledException && !cancellationToken.IsCancellationRequested)
        {
            return new(false, "mission_authority_unavailable");
        }
    }
}
