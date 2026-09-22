using System.Net;
using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

public sealed record CoordinatorQuicSettings(
    IPEndPoint ListenEndPoint,
    string ServerCertificatePath,
    string? ServerCertificatePassword,
    string ClientCaCertificatePath,
    ClusterHello LocalHello,
    string[] RequiredCapabilities)
{
    public const string Alpn = "lancer-nexus-control/1";

    public static CoordinatorQuicSettings? FromConfiguration(IConfiguration configuration)
    {
        if (!configuration.GetValue<bool>("Coordinator:Quic:Enabled"))
            return null;

        var addressValue = configuration["Coordinator:Quic:ListenAddress"] ?? "127.0.0.1";
        if (!IPAddress.TryParse(addressValue, out var address))
            throw new InvalidOperationException("Coordinator:Quic:ListenAddress must be an IP address.");

        var port = configuration.GetValue<int?>("Coordinator:Quic:Port") ?? 7443;
        if (port is < 1 or > 65535)
            throw new InvalidOperationException("Coordinator:Quic:Port must be between 1 and 65535.");

        var nodeId = configuration["Coordinator:Quic:NodeId"];
        var certificatePath = configuration["Coordinator:Quic:ServerCertificatePath"];
        var caPath = configuration["Coordinator:Quic:ClientCaCertificatePath"];
        if (string.IsNullOrWhiteSpace(nodeId) || string.IsNullOrWhiteSpace(certificatePath) ||
            string.IsNullOrWhiteSpace(caPath))
            throw new InvalidOperationException(
                "Enabling Coordinator QUIC requires NodeId, ServerCertificatePath and ClientCaCertificatePath.");

        var requiredCapabilities = configuration.GetSection("Coordinator:Quic:RequiredCapabilities").Get<string[]>() ?? [];
        if (requiredCapabilities.Any(string.IsNullOrWhiteSpace) ||
            requiredCapabilities.Distinct(StringComparer.Ordinal).Count() != requiredCapabilities.Length)
            throw new InvalidOperationException("Coordinator QUIC required capabilities must be non-empty and unique.");

        return new CoordinatorQuicSettings(
            new IPEndPoint(address, port),
            Path.GetFullPath(certificatePath),
            configuration["Coordinator:Quic:ServerCertificatePassword"],
            Path.GetFullPath(caPath),
            new ClusterHello
            {
                NodeId = nodeId,
                InstanceId = configuration["Coordinator:Quic:InstanceId"] ?? nodeId,
                BuildVersion = configuration["Coordinator:Quic:BuildVersion"] ??
                               typeof(CoordinatorQuicSettings).Assembly.GetName().Version?.ToString() ?? "unknown",
                Capabilities = ["cluster_handshake_v1", "agent_heartbeat_v1", "instance_heartbeat_v1"]
            },
            requiredCapabilities);
    }
}
