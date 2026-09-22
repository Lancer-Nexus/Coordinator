using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using LancerNexus.Coordinator;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MtlsPeerCertificateValidatorTests
{
    [Fact]
    public void Validate_AcceptsClientCertificateFromConfiguredCaAndReadsSanIdentity()
    {
        using var authority = CreateAuthority("Lancer Nexus Test CA");
        using var peer = CreateClientCertificate(authority, "agent-01", includeDnsSan: true);
        var validator = new MtlsPeerCertificateValidator(authority);

        var sanExtension = peer.Extensions.Cast<X509Extension>().Single(extension => extension.Oid?.Value == "2.5.29.17");
        Assert.Equal("agent-01", new X509SubjectAlternativeNameExtension(sanExtension.RawData, sanExtension.Critical)
            .EnumerateDnsNames().Single());
        Assert.True(validator.Validate(peer, SslPolicyErrors.None));
        Assert.Equal("agent-01", MtlsPeerCertificateValidator.GetNodeId(peer));
    }

    [Fact]
    public void Validate_RejectsUntrustedCertificateAndIdentityWithoutDnsSan()
    {
        using var trustedAuthority = CreateAuthority("Trusted CA");
        using var untrustedAuthority = CreateAuthority("Untrusted CA");
        using var untrustedPeer = CreateClientCertificate(untrustedAuthority, "agent-02", includeDnsSan: true);
        using var peerWithoutSan = CreateClientCertificate(trustedAuthority, "agent-03", includeDnsSan: false);
        var validator = new MtlsPeerCertificateValidator(trustedAuthority);

        Assert.False(validator.Validate(untrustedPeer, SslPolicyErrors.None));
        Assert.False(validator.Validate(null, SslPolicyErrors.RemoteCertificateNotAvailable));
        Assert.Null(MtlsPeerCertificateValidator.GetNodeId(peerWithoutSan));
    }

    private static X509Certificate2 CreateAuthority(string commonName)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={commonName}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
    }

    private static X509Certificate2 CreateClientCertificate(
        X509Certificate2 authority,
        string identity,
        bool includeDnsSan)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={identity}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, true));
        var usages = new OidCollection { new("1.3.6.1.5.5.7.3.2") };
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(usages, true));
        if (includeDnsSan)
        {
            var subjectAlternativeNames = new SubjectAlternativeNameBuilder();
            subjectAlternativeNames.AddDnsName(identity);
            request.CertificateExtensions.Add(subjectAlternativeNames.Build());
        }

        return request.Create(
            authority,
            DateTimeOffset.UtcNow.AddMinutes(-1),
            DateTimeOffset.UtcNow.AddDays(1),
            RandomNumberGenerator.GetBytes(16));
    }
}
