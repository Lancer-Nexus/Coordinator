using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MarketReputationPricePolicyTests
{
    [Fact]
    public void PositiveReputationDiscountsPurchasesAndImprovesSalePayouts()
    {
        Assert.True(MarketReputationPricePolicy.TryGetFactor(MarketTradeSideV1.Buy, 1f,
            out var buyFactor, out var buyPpm));
        Assert.True(MarketReputationPricePolicy.TryGetFactor(MarketTradeSideV1.Sell, 1f,
            out var sellFactor, out var sellPpm));
        Assert.Equal(0.9m, buyFactor);
        Assert.Equal(900_000, buyPpm);
        Assert.Equal(1.1m, sellFactor);
        Assert.Equal(1_100_000, sellPpm);
    }

    [Fact]
    public void NegativeReputationRaisesPurchasePricesAndLowersSalePayouts()
    {
        Assert.True(MarketReputationPricePolicy.TryGetFactor(MarketTradeSideV1.Buy, -1f,
            out var buyFactor, out _));
        Assert.True(MarketReputationPricePolicy.TryGetFactor(MarketTradeSideV1.Sell, -1f,
            out var sellFactor, out _));
        Assert.Equal(1.1m, buyFactor);
        Assert.Equal(0.9m, sellFactor);
    }

    [Fact]
    public void RejectsInvalidReputationAndAppliesFactorAfterEachStockStep()
    {
        Assert.False(MarketReputationPricePolicy.TryGetFactor(MarketTradeSideV1.Buy, float.NaN,
            out _, out _));
        Assert.True(MarketPriceCalculator.TryCalculateTradeTotal(100, 20, 20,
            MarketTradeSideV1.Buy, 3, out var firstPrice, out var total, 0.95m));
        Assert.Equal(95, firstPrice);
        Assert.Equal(300, total);
    }
}
