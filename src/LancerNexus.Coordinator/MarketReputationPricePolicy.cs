using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

/// <summary>Initial bounded reputation policy used for personal quote previews.</summary>
internal static class MarketReputationPricePolicy
{
    public const int Version = 1;
    public const decimal EffectPerReputationPoint = 0.10m;

    public static bool TryGetFactor(MarketTradeSideV1 side, float reputation,
        out decimal factor, out int factorPartsPerMillion)
    {
        factor = 1m;
        factorPartsPerMillion = 1_000_000;
        if (!Enum.IsDefined(side) || !float.IsFinite(reputation) || reputation is < -1f or > 1f)
            return false;
        var value = (decimal)reputation;
        factor = side == MarketTradeSideV1.Buy
            ? 1m - value * EffectPerReputationPoint
            : 1m + value * EffectPerReputationPoint;
        factor = Math.Clamp(factor, 0.9m, 1.1m);
        factorPartsPerMillion = checked((int)decimal.Round(factor * 1_000_000m, 0,
            MidpointRounding.AwayFromZero));
        factor = factorPartsPerMillion / 1_000_000m;
        return true;
    }
}
