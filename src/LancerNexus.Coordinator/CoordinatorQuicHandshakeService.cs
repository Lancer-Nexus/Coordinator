using System.Buffers;
using System.Net.Quic;
using System.Net.Security;
using System.Runtime.Versioning;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using LancerNexus.Protocol;
using MessagePack;

namespace LancerNexus.Coordinator;

[SupportedOSPlatform("linux")]
public sealed class CoordinatorQuicHandshakeService : BackgroundService
{
    private const long ProtocolStreamErrorCode = 0x100;
    private const long ProtocolConnectionErrorCode = 0x101;
    private const int MaxSerializedEnvelopeLength = checked((int)ClusterEnvelopeValidator.MaxPayloadLength + 16 * 1024);
    private static readonly SslApplicationProtocol Alpn = new(CoordinatorQuicSettings.Alpn);
    private static readonly MessagePackSerializerOptions UntrustedMessagePack =
        MessagePackSerializerOptions.Standard.WithSecurity(MessagePackSecurity.UntrustedData);

    private readonly CoordinatorQuicSettings settings;
    private readonly ILogger<CoordinatorQuicHandshakeService> logger;
    private readonly X509Certificate2 serverCertificate;
    private readonly X509Certificate2 clientCaCertificate;
    private readonly MtlsPeerCertificateValidator peerCertificateValidator;
    private readonly SemaphoreSlim activeConnectionLimit = new(32, 32);

    public CoordinatorQuicHandshakeService(
        CoordinatorQuicSettings settings,
        ILogger<CoordinatorQuicHandshakeService> logger)
    {
        this.settings = settings;
        this.logger = logger;

        serverCertificate = X509CertificateLoader.LoadPkcs12FromFile(
            settings.ServerCertificatePath,
            settings.ServerCertificatePassword,
            X509KeyStorageFlags.EphemeralKeySet);
        clientCaCertificate = X509CertificateLoader.LoadCertificateFromFile(settings.ClientCaCertificatePath);
        peerCertificateValidator = new MtlsPeerCertificateValidator(clientCaCertificate);
        ValidateServerCertificate(serverCertificate);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!QuicListener.IsSupported)
            throw new PlatformNotSupportedException(
                "Coordinator QUIC is enabled but System.Net.Quic is unsupported; install libmsquic and enable TLS 1.3.");

        var authenticationOptions = new SslServerAuthenticationOptions
        {
            ApplicationProtocols = [Alpn],
            ClientCertificateRequired = true,
            EnabledSslProtocols = SslProtocols.Tls13,
            ServerCertificate = serverCertificate,
            RemoteCertificateValidationCallback = (_, certificate, _, errors) =>
                peerCertificateValidator.Validate(certificate, errors)
        };

        var listenerOptions = new QuicListenerOptions
        {
            ListenEndPoint = settings.ListenEndPoint,
            ListenBacklog = 64,
            ApplicationProtocols = [Alpn],
            ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(new QuicServerConnectionOptions
            {
                DefaultStreamErrorCode = ProtocolStreamErrorCode,
                DefaultCloseErrorCode = ProtocolConnectionErrorCode,
                MaxInboundBidirectionalStreams = 1,
                ServerAuthenticationOptions = authenticationOptions
            })
        };

        await using var listener = await QuicListener.ListenAsync(listenerOptions, stoppingToken);
        logger.LogInformation("Coordinator mTLS QUIC handshake listener bound to {EndPoint}", listener.LocalEndPoint);

        var activeTasks = new HashSet<Task>();
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await activeConnectionLimit.WaitAsync(stoppingToken);
                QuicConnection connection;
                try
                {
                    connection = await listener.AcceptConnectionAsync(stoppingToken);
                }
                catch
                {
                    activeConnectionLimit.Release();
                    throw;
                }

                activeTasks.RemoveWhere(task => task.IsCompleted);
                activeTasks.Add(HandleConnectionAsync(connection, stoppingToken));
            }
        }
        finally
        {
            await Task.WhenAll(activeTasks);
        }
    }

    public override void Dispose()
    {
        serverCertificate.Dispose();
        clientCaCertificate.Dispose();
        activeConnectionLimit.Dispose();
        base.Dispose();
    }

    private async Task HandleConnectionAsync(QuicConnection connection, CancellationToken stoppingToken)
    {
        await using (connection)
        {
            try
            {
                using var handshakeTimeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                handshakeTimeout.CancelAfter(TimeSpan.FromSeconds(10));
                var certificate = connection.RemoteCertificate;
                var certificateNodeId = MtlsPeerCertificateValidator.GetNodeId(certificate);
                if (certificateNodeId is null)
                {
                    logger.LogWarning("Coordinator rejected a QUIC peer without exactly one DNS SAN identity");
                    return;
                }

                await using var stream = await connection.AcceptInboundStreamAsync(handshakeTimeout.Token);
                if (stream.Type != QuicStreamType.Bidirectional)
                    throw new ProtocolViolationException("The handshake requires a bidirectional stream.");

                var request = await ReadEnvelopeAsync(stream, handshakeTimeout.Token);
                ClusterEnvelopeValidator.Validate(request);
                if (request.MessageType != (ushort)ClusterMessageType.Hello || request.Flags != ClusterFrameFlags.Request)
                    throw new ProtocolViolationException("The first QUIC stream must contain a Hello request.");

                var peerHello = MessagePackSerializer.Deserialize<ClusterHello>(request.Payload, UntrustedMessagePack);
                ClusterHandshakeResult result;
                if (!string.Equals(certificateNodeId, peerHello.NodeId, StringComparison.OrdinalIgnoreCase))
                {
                    result = new ClusterHandshakeResult(false, "certificate_identity_mismatch", [], []);
                }
                else
                {
                    result = ClusterHandshakeNegotiator.Negotiate(
                        settings.LocalHello,
                        peerHello,
                        settings.RequiredCapabilities);
                }

                var responsePayload = MessagePackSerializer.Serialize(new ClusterHandshakeResponse
                {
                    Accepted = result.Accepted,
                    ReasonCode = result.ReasonCode,
                    NegotiatedCapabilities = result.NegotiatedCapabilities
                });
                var response = new ClusterEnvelope
                {
                    MessageType = (ushort)ClusterMessageType.Hello,
                    Flags = result.Accepted ? ClusterFrameFlags.Response : ClusterFrameFlags.Response | ClusterFrameFlags.Error,
                    CorrelationId = request.CorrelationId,
                    Sequence = request.Sequence,
                    PayloadLength = checked((uint)responsePayload.Length),
                    Payload = responsePayload
                };
                await stream.WriteAsync(MessagePackSerializer.Serialize(response), stoppingToken);
                stream.CompleteWrites();

                logger.LogInformation(
                    "Coordinator QUIC handshake {Result} for peer {NodeId} from {RemoteEndPoint}",
                    result.ReasonCode,
                    certificateNodeId ?? "<missing-san>",
                    connection.RemoteEndPoint);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Normal host shutdown.
            }
            catch (Exception exception)
            {
                logger.LogWarning(exception, "Coordinator rejected or lost a QUIC handshake from {RemoteEndPoint}",
                    connection.RemoteEndPoint);
            }
            finally
            {
                activeConnectionLimit.Release();
            }
        }
    }

    private static async Task<ClusterEnvelope> ReadEnvelopeAsync(QuicStream stream, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();
        var chunk = ArrayPool<byte>.Shared.Rent(8192);
        try
        {
            int read;
            while ((read = await stream.ReadAsync(chunk.AsMemory(), cancellationToken)) != 0)
            {
                if (buffer.Length + read > MaxSerializedEnvelopeLength)
                    throw new ProtocolViolationException("QUIC handshake frame exceeds the maximum size.");
                buffer.Write(chunk, 0, read);
            }

            if (buffer.Length == 0)
                throw new ProtocolViolationException("QUIC handshake stream was empty.");
            return MessagePackSerializer.Deserialize<ClusterEnvelope>(buffer.ToArray(), UntrustedMessagePack);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunk);
        }
    }

    private static void ValidateServerCertificate(X509Certificate2 certificate)
    {
        if (!certificate.HasPrivateKey || certificate.NotBefore.ToUniversalTime() > DateTime.UtcNow ||
            certificate.NotAfter.ToUniversalTime() <= DateTime.UtcNow)
            throw new CryptographicException("The configured QUIC server certificate is invalid or has no private key.");

        var hasServerAuthenticationUsage = certificate.Extensions
            .Where(extension => extension.Oid?.Value == "2.5.29.37")
            .Select(extension => new X509EnhancedKeyUsageExtension(extension, extension.Critical))
            .Any(extension => extension.EnhancedKeyUsages
                .Cast<Oid>()
                .Any(oid => oid.Value == "1.3.6.1.5.5.7.3.1"));
        if (!hasServerAuthenticationUsage)
            throw new CryptographicException("The configured QUIC server certificate must allow TLS server authentication.");
    }
}
