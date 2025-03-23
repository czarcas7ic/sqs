# Cyclic Arb Plugin

## Overview

The Cyclic Arbitrage Plugin is a component designed to identify and execute profitable trading opportunities across osmosis liquidity pools. It automatically detects price inefficiencies in the marketplace and performs trades to capture arbitrage profit.

## How It Works

Cyclic arbitrage is based on a simple principle:

- Start with an amount of Token A
- Swap to Token B through a liquidity pool
- Find a route to swap back to Token A (potentially through different pools)
- If you end up with more Token A than you started with, execute the trade cycle

The plugin continuously monitors pools for these opportunities at the end of each block, using binary search algorithms to find the optimal trade size that maximizes profit.

## Architecture

The plugin operates as an EndBlockProcessPlugin, triggering after each block is processed. Key components include:

- Pool Monitoring: Tracks changes in liquidity pools to identify potential trading opportunities
- Route Finding: Discovers optimal paths for completing the token swap cycle
- Profit Optimization: Binary search algorithm to determine ideal trade sizes
- Transaction Execution: Simulates and executes profitable trades

### Optimal Size Finding

The plugin employs recursive binary search to find the most profitable trade amount:

- Starts with a proposed amount
- Checks if profitable
- Expands search space up to 5% larger
- Uses maximum 15 recursive attempts to optimize
- Compares profitability by estimating fees and returns

## Performance Optimizations

The plugin implements several performance optimizations:

### Concurrency Control with Semaphores

A semaphore pattern limits the number of concurrent operations:

```go
maxWorkers := runtime.NumCPU() * 2
semaphore := make(chan struct{}, maxWorkers)

// To acquire the semaphore:
semaphore <- struct{}{}

// To release the semaphore:
<-semaphore
```

This prevents resource exhaustion when processing many pools simultaneously while maintaining high throughput.

## Future Enhancements

- What do we want?
- Concurrency of processing pools
- Lock on a single signer as to avoid nonce issues
- Signer queue so that we do not get bottlecked on one signer (optional)
- Do not process all pools only the ones that have relevant token balance
- Avoid arbing the same denom multiples times within a block.