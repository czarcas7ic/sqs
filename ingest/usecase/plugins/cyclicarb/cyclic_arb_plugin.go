package cyclicarb

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/osmosis-labs/osmosis/osmomath"
	cosmosbroadcast "github.com/osmosis-labs/osmoutil-go/tx/broadcast/cosmos"
	"github.com/osmosis-labs/sqs/domain"
	"github.com/osmosis-labs/sqs/domain/mvc"
	orderbookplugindomain "github.com/osmosis-labs/sqs/domain/orderbook/plugin"
	passthroughdomain "github.com/osmosis-labs/sqs/domain/passthrough"
	blockctx "github.com/osmosis-labs/sqs/ingest/usecase/plugins/orderbook/fillbot/context/block"
	msgctx "github.com/osmosis-labs/sqs/ingest/usecase/plugins/orderbook/fillbot/context/msg"
	"github.com/osmosis-labs/sqs/log"
	"github.com/osmosis-labs/sqs/tokens/usecase/pricing/worker"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

// cyclicArbPlugin is a plugin that fills the orderbook orders at the end of the block.
type cyclicArbPlugin struct {
	poolsUseCase  mvc.PoolsUsecase
	routerUseCase mvc.RouterUsecase
	tokensUseCase mvc.TokensUsecase

	liquidityPricer domain.LiquidityPricer

	passthroughGRPCClient passthroughdomain.PassthroughGRPCClient

	orderbookCWAAPIClient orderbookplugindomain.OrderbookCWAPIClient

	atomicBool atomic.Bool

	orderMapByPoolID  sync.Map
	defaultQuoteDenom string

	supportedAssets map[string]struct{}

	signer cosmosbroadcast.CosmosSigner

	logger log.Logger
}

var _ domain.EndBlockProcessPlugin = &cyclicArbPlugin{}

type orderBookProcessResult struct {
	err    error
	poolID uint64
}

const (
	tracerName = "sqs-orderbook-filler"

	CyclicArbPluginName = "cyclic-arb-sqs-plugin"
)

var (
	tracer = otel.Tracer(tracerName)
)

func New(poolsUseCase mvc.PoolsUsecase, routerUseCase mvc.RouterUsecase, tokensUseCase mvc.TokensUsecase, passthroughGRPCClient passthroughdomain.PassthroughGRPCClient, orderBookCWAPIClient orderbookplugindomain.OrderbookCWAPIClient, defaultQuoteDenom string, cosmosSigner cosmosbroadcast.CosmosSigner, logger log.Logger) *cyclicArbPlugin {
	liquidityPricer := worker.NewLiquidityPricer(defaultQuoteDenom, tokensUseCase.GetChainScalingFactorByDenomMut)

	return &cyclicArbPlugin{
		poolsUseCase:  poolsUseCase,
		routerUseCase: routerUseCase,
		tokensUseCase: tokensUseCase,

		passthroughGRPCClient: passthroughGRPCClient,
		orderbookCWAAPIClient: orderBookCWAPIClient,

		atomicBool: atomic.Bool{},

		orderMapByPoolID: sync.Map{},

		defaultQuoteDenom: defaultQuoteDenom,

		liquidityPricer: liquidityPricer,

		supportedAssets: map[string]struct{}{
			"uosmo": {},
			"ibc/498A0751C798A0D9A389AA3691123DADA57DAA4FE165D5C75894505B876BA6E4": {},
		},

		signer: cosmosSigner,

		logger: logger,
	}
}

// ProcessEndBlock implements domain.EndBlockProcessPlugin.
func (o *cyclicArbPlugin) ProcessEndBlock(ctx context.Context, blockHeight uint64, metadata domain.BlockPoolMetadata) error {
	const (
		poolIDMonitored = 1464
	)

	// Iterate over pool IDs mo
	for poolID := range metadata.PoolIDs {
		if poolID == poolIDMonitored {
			o.logger.Info("pool ID monitored was changed", zap.Uint64("pool_id", poolID))
		}
	}

	// For simplicity, we allow only one block to be processed at a time.
	// This may be relaxed in the future.
	if !o.atomicBool.CompareAndSwap(false, true) {
		o.logger.Info("orderbook filler is already in progress", zap.Uint64("block_height", blockHeight))
		return nil
	}
	defer o.atomicBool.Store(false)

	// Get bot balances
	balances, err := o.passthroughGRPCClient.AllBalances(ctx, o.signer.GetAddressString())
	if err != nil {
		return err
	}

	uniqueDenoms := []string{
		"uosmo",
		"ibc/498A0751C798A0D9A389AA3691123DADA57DAA4FE165D5C75894505B876BA6E4",
	}

	// Get prices for all the unique denoms in the orderbook, including base denom.
	orderBookDenomPrices, err := o.tokensUseCase.GetPrices(ctx, uniqueDenoms, []string{o.defaultQuoteDenom}, domain.ChainPricingSourceType)
	if err != nil {
		return err
	}

	// Configure block context
	blockCtx, err := blockctx.New(ctx, uniqueDenoms, orderBookDenomPrices, balances, o.defaultQuoteDenom, blockHeight)
	if err != nil {
		return err
	}

	proposedAmountIn := osmomath.NewInt(5_000_000)

	wg := sync.WaitGroup{}

	for poolID := range metadata.PoolIDs {
		wg.Add(2)

		go func(poolID uint64) {

			// Get pool
			pool, err := o.poolsUseCase.GetPool(poolID)
			if err != nil {
				return
			}

			denoms := pool.GetPoolDenoms()

			go func(denomIn, denomOut string) {
				defer wg.Done()

				// NOTE: if we get here we have simulated a profitable arb.
				value, err := o.computePerfectArbAmountIfExists(blockCtx, proposedAmountIn, denomIn, denomOut, poolID)
				if err != nil {
					o.logger.Error("failed to compute perfect arb amount", zap.Error(err))
					return
				}

				o.logger.Info("computed perfect arb amount", zap.Uint64("pool_id", poolID), zap.String("denom_in", denomIn), zap.String("denom_out", denomOut), zap.Int64("amount", value.Int64()))
			}(denoms[0], denoms[1])

			go func(denomIn, denomOut string) {
				defer wg.Done()

				value, err := o.computePerfectArbAmountIfExists(blockCtx, proposedAmountIn, denomIn, denomOut, poolID)
				if err != nil {
					o.logger.Error("failed to compute perfect arb amount", zap.Error(err))
					return
				}

				// NOTE: if we get here we have simulated a profitable arb.
				o.logger.Info("computed perfect arb amount", zap.Uint64("pool_id", poolID), zap.String("denom_in", denomIn), zap.String("denom_out", denomOut), zap.Int64("amount", value.Int64()))
			}(denoms[1], denoms[0])
		}(poolID)
	}

	o.logger.Info("processed end block in orderbook filler ingest plugin", zap.Uint64("block_height", blockHeight))
	return nil
}

const (
	maxRecursionAttemptsArbSearch = 15
)

var (
	multiplier = osmomath.MustNewDecFromStr("1.05")
	two        = osmomath.MustNewDecFromStr("2")
)

// computePerfectArbAmountIfExists computes the perfect arb amount if it exists by performing binary search.
// It tries to prefer a higher amount if it exists in order to fill all orders in-full while maximizing profit.
// nolint: unparam
func (o *cyclicArbPlugin) computePerfectArbAmountIfExists(ctx blockctx.BlockCtxI, proposedAmountIn osmomath.Int, denomIn, denomOut string, orderBookID uint64) (osmomath.Int, error) {
	// If the initial proposed amount in is not valid, return error.
	msgCtx, err := o.validateArb(ctx, proposedAmountIn, denomIn, denomOut, orderBookID)
	if err != nil {
		return osmomath.Int{}, err
	}

	// Otherwise, try to find a higher amount such that it fills all orders in-full and is profitable.
	amountInHigh := proposedAmountIn.ToLegacyDec().MulMut(multiplier).TruncateInt()

	msgCtx, result, err := o.tryValidate(ctx, proposedAmountIn, amountInHigh, denomIn, denomOut, orderBookID, msgCtx, maxRecursionAttemptsArbSearch)
	if err != nil {
		return proposedAmountIn, nil
	}

	// If profitable, execute add the message to the transaction context
	txCtx := ctx.GetTxCtx()
	txCtx.AddMsg(msgCtx)

	return result, nil
}

func (o *cyclicArbPlugin) tryValidate(ctx blockctx.BlockCtxI, amountInLow osmomath.Int, amountInHigh osmomath.Int, denomIn, denomOut string, orderBookID uint64, lowMsgCtx msgctx.MsgContextI, attemptsRemaining int) (msgctx.MsgContextI, osmomath.Int, error) {
	if attemptsRemaining == 0 {
		return lowMsgCtx, amountInLow, nil
	}

	mid := amountInLow.ToLegacyDec().Add(amountInHigh.ToLegacyDec()).QuoRoundupMut(two).Ceil().TruncateInt()

	// Case 1: mid arb works => recurse into (mid, high)
	midMsgCtx, err := o.validateArb(ctx, mid, denomIn, denomOut, orderBookID)
	if err == nil && midMsgCtx.GetMaxFeeCap().GTE(lowMsgCtx.GetMaxFeeCap()) {
		return o.tryValidate(ctx, mid, amountInHigh, denomIn, denomOut, orderBookID, midMsgCtx, attemptsRemaining-1)
	}

	// Case 2: mid arb doesn't work => recurse into (low, mid)
	topMsgCtx, topAmount, err := o.tryValidate(ctx, amountInLow, mid, denomIn, denomOut, orderBookID, lowMsgCtx, attemptsRemaining-1)
	if err == nil && topMsgCtx.GetMaxFeeCap().GTE(lowMsgCtx.GetMaxFeeCap()) {
		return topMsgCtx, topAmount, nil
	}

	// Case 3: all attempts failed but low arb has been validated in the caller => return it.
	return lowMsgCtx, amountInLow, nil
}

// tryFill tries to fill the orderbook by executing the transaction.
// It ranks and filters the pools, simulates the transaction messages, and executes the swap if the simulation passes.
func (o *cyclicArbPlugin) tryFill(ctx blockctx.BlockCtxI) error {
	txCtx := ctx.GetTxCtx()
	msgs := txCtx.GetSDKMsgs()

	if len(msgs) == 0 {
		return nil
	}

	// Rank and filter pools
	txCtx.RankAndFilterMsgs()

	// Simulate transaction messages
	sdkMsgs := txCtx.GetSDKMsgs()
	_, adjustedGasAmount, err := o.simulateMsgs(ctx.AsGoCtx(), sdkMsgs)
	if err != nil {
		return err
	}

	// Update adjusted gas amount upon resimulating the transaction.
	txCtx.UpdateAdjustedGasTotal(adjustedGasAmount)

	// Execute the swap
	_, _, err = o.executeTx(ctx)
	if err != nil {
		return err
	}

	return nil
}
