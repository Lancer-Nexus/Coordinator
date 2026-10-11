using System.Numerics;

namespace LancerNexus.Coordinator;

internal static class MarketPriceCalculator
{
    public static long Calculate(long basePrice, long targetStock, long availableStock)
    {
        if (basePrice == 0)
            return 0;
        var floorStock = Math.Max(1, targetStock / 4 + (targetStock % 4 == 0 ? 0 : 1));
        var denominator = Math.Max(availableStock, floorStock);
        var factor = Math.Clamp((decimal)targetStock / denominator, 0.25m, 4m);
        var price = decimal.Round(basePrice * factor, 0, MidpointRounding.AwayFromZero);
        return (long)BigInteger.Min((BigInteger)long.MaxValue, new BigInteger(price));
    }
}
