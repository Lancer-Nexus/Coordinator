using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace LancerNexus.Coordinator;

public sealed class MtlsPeerCertificateValidator
{
    private const string ClientAuthenticationOid = "1.3.6.1.5.5.7.3.2";
    private readonly X509Certificate2 trustedRoot;

    public MtlsPeerCertificateValidator(X509Certificate2 trustedRoot)
    {
        this.trustedRoot = trustedRoot ?? throw new ArgumentNullException(nameof(trustedRoot));
        var isCertificateAuthority = trustedRoot.Extensions
            .Where(extension => extension.Oid?.Value == "2.5.29.19")
            .Select(extension => new X509BasicConstraintsExtension(extension, extension.Critical))
            .Any(extension => extension.CertificateAuthority);
        if (!isCertificateAuthority || trustedRoot.NotBefore.ToUniversalTime() > DateTime.UtcNow ||
            trustedRoot.NotAfter.ToUniversalTime() <= DateTime.UtcNow)
            throw new CryptographicException("The configured client trust certificate is not a CA certificate.");
    }

    public bool Validate(X509Certificate? certificate, SslPolicyErrors policyErrors)
    {
        if (certificate is null ||
            (policyErrors & (SslPolicyErrors.RemoteCertificateNotAvailable |
                             SslPolicyErrors.RemoteCertificateNameMismatch)) != 0)
            return false;

        using var leaf = X509CertificateLoader.LoadCertificate(certificate.Export(X509ContentType.Cert));
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(trustedRoot);
        chain.ChainPolicy.ApplicationPolicy.Add(new Oid(ClientAuthenticationOid));
        // Private-cluster certificates are expected to be short-lived and rotated; revocation
        // distribution is not currently configured for this listener.
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.VerificationFlags = X509VerificationFlags.NoFlag;
        return chain.Build(leaf);
    }

    public static string? GetNodeId(X509Certificate? certificate)
    {
        if (certificate is null)
            return null;

        using var leaf = X509CertificateLoader.LoadCertificate(certificate.Export(X509ContentType.Cert));
        try
        {
            var dnsNames = leaf.Extensions
                .Where(extension => extension.Oid?.Value == "2.5.29.17")
                .SelectMany(extension => new X509SubjectAlternativeNameExtension(
                    extension.RawData,
                    extension.Critical).EnumerateDnsNames())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();
            return dnsNames.Length == 1 && !dnsNames[0].Contains('*', StringComparison.Ordinal)
                ? dnsNames[0]
                : null;
        }
        catch (CryptographicException)
        {
            return null;
        }
    }
}
