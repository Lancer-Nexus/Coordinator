using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MarketPriceCalculatorTests
{
    [Fact]
    public void PurchaseTotalUsesSuccessivelyScarcerStockPrices()
    {
        Assert.True(MarketPriceCalculator.TryCalculateTradeTotal(100, 20, 20,
            MarketTradeSideV1.Buy, 5, out var firstUnitPrice, out var total));
        Assert.Equal(100, firstUnitPrice);
        Assert.Equal(559, total);
    }

    [Fact]
    public void SaleTotalUsesSuccessivelyMoreAvailableStockPrices()
    {
        Assert.True(MarketPriceCalculator.TryCalculateTradeTotal(100, 20, 20,
            MarketTradeSideV1.Sell, 5, out var firstUnitPrice, out var total));
        Assert.Equal(100, firstUnitPrice);
        Assert.Equal(456, total);
    }

    [Fact]
    public void RejectsUnavailableStockInvalidQuantitiesAndTotalsThatOverflow()
    {
        Assert.False(MarketPriceCalculator.TryCalculateTradeTotal(100, 20, 4,
            MarketTradeSideV1.Buy, 5, out _, out _));
        Assert.False(MarketPriceCalculator.TryCalculateTradeTotal(100, 20, 20,
            MarketTradeSideV1.Sell, 0, out _, out _));
        Assert.False(MarketPriceCalculator.TryCalculateTradeTotal(long.MaxValue, 20, 20,
            MarketTradeSideV1.Sell, 2, out _, out _));
    }
}
