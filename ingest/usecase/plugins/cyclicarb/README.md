# Cyclic Arb Plugin


- What do we want?

- Concurrency of processing pools

- Lock on a single signer as to avoid nonce issues

- Signer queue so that we do not get bottlecked on one signer (optional)

- Do not process all pools only the ones that have relevant token balance

- Avoid arbing the same denom multiples times within a block.