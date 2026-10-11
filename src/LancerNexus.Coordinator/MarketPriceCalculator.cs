using System.Numerics;
using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

internal static class MarketPriceCalculator
{
    public static bool TryCalculateTradeTotal(long basePrice, long targetStock, long availableStock,
        MarketTradeSideV1 side, long quantity, out long firstUnitPrice, out long totalPrice,
        decimal personalPriceFactor = 1m)
    {
        firstUnitPrice = 0;
        totalPrice = 0;
        if (basePrice < 0 || targetStock <= 0 || availableStock < 0 ||
            quantity is <= 0 or > MarketTradeEventV1.MaximumQuantity || !Enum.IsDefined(side) ||
            personalPriceFactor is < 0.75m or > 1.25m ||
            side == MarketTradeSideV1.Buy && quantity > availableStock ||
            side == MarketTradeSideV1.Sell && quantity > long.MaxValue - availableStock)
            return false;

        var projectedStock = availableStock;
        for (long unit = 0; unit < quantity; unit++)
        {
            var marketUnitPrice = Calculate(basePrice, targetStock, projectedStock);
            var adjustedPrice = decimal.Round(marketUnitPrice * personalPriceFactor, 0,
                MidpointRounding.AwayFromZero);
            if (adjustedPrice > long.MaxValue)
                return false;
            var unitPrice = (long)adjustedPrice;
            if (unit == 0)
                firstUnitPrice = unitPrice;
            if (totalPrice > long.MaxValue - unitPrice)
                return false;
            totalPrice += unitPrice;
            projectedStock += side == MarketTradeSideV1.Buy ? -1 : 1;
        }
        return true;
    }

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
