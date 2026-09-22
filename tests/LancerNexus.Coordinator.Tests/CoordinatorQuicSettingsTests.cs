using LancerNexus.Coordinator;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class CoordinatorQuicSettingsTests
{
    [Fact]
    public void FromConfiguration_DisablesListenerUnlessExplicitlyEnabled()
    {
        var configuration = new ConfigurationBuilder().Build();

        Assert.Null(CoordinatorQuicSettings.FromConfiguration(configuration));
    }

    [Fact]
    public void FromConfiguration_RequiresCertificateAndStableIdentityWhenEnabled()
    {
        var configuration = Configuration(new Dictionary<string, string?>
        {
            ["Coordinator:Quic:Enabled"] = "true"
        });

        var error = Assert.Throws<InvalidOperationException>(() =>
            CoordinatorQuicSettings.FromConfiguration(configuration));

        Assert.Contains("NodeId", error.Message, StringComparison.Ordinal);
        Assert.Contains("ClientCaCertificatePath", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FromConfiguration_BindsPrivateEndpointAndRequiredCapabilities()
    {
        var configuration = Configuration(new Dictionary<string, string?>
        {
            ["Coordinator:Quic:Enabled"] = "true",
            ["Coordinator:Quic:NodeId"] = "coordinator-01",
            ["Coordinator:Quic:ListenAddress"] = "10.20.0.20",
            ["Coordinator:Quic:Port"] = "7443",
            ["Coordinator:Quic:ServerCertificatePath"] = "/etc/lancer-nexus/coordinator.pfx",
            ["Coordinator:Quic:ClientCaCertificatePath"] = "/etc/lancer-nexus/cluster-ca.crt",
            ["Coordinator:Quic:RequiredCapabilities:0"] = "cluster_handshake_v1"
        });

        var settings = CoordinatorQuicSettings.FromConfiguration(configuration);

        Assert.NotNull(settings);
        Assert.Equal("10.20.0.20", settings.ListenEndPoint.Address.ToString());
        Assert.Equal(7443, settings.ListenEndPoint.Port);
        Assert.Equal("coordinator-01", settings.LocalHello.NodeId);
        Assert.Equal("cluster_handshake_v1", Assert.Single(settings.RequiredCapabilities));
    }

    [Fact]
    public void FromConfiguration_RejectsInvalidBindAddressAndPort()
    {
        var addressValues = BaseQuicConfiguration();
        addressValues["Coordinator:Quic:ListenAddress"] = "coordinator.internal";
        var portValues = BaseQuicConfiguration();
        portValues["Coordinator:Quic:Port"] = "70000";
        var invalidAddress = Configuration(addressValues);
        var invalidPort = Configuration(portValues);

        Assert.Throws<InvalidOperationException>(() => CoordinatorQuicSettings.FromConfiguration(invalidAddress));
        Assert.Throws<InvalidOperationException>(() => CoordinatorQuicSettings.FromConfiguration(invalidPort));
    }

    private static IConfiguration Configuration(IDictionary<string, string?> values) =>
        new ConfigurationBuilder().AddInMemoryCollection(values).Build();

    private static Dictionary<string, string?> BaseQuicConfiguration() => new()
    {
        ["Coordinator:Quic:Enabled"] = "true",
        ["Coordinator:Quic:NodeId"] = "coordinator-01",
        ["Coordinator:Quic:ServerCertificatePath"] = "/etc/lancer-nexus/coordinator.pfx",
        ["Coordinator:Quic:ClientCaCertificatePath"] = "/etc/lancer-nexus/cluster-ca.crt"
    };
}
