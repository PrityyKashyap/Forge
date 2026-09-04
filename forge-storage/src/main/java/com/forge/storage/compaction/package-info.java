/**
 * Phase 13 compaction: merging multiple SSTables into one to bound SSTable
 * count and reclaim space from overwritten/deleted keys. See
 * {@link com.forge.storage.compaction.Compactor} for the merge algorithm and
 * strategy, and {@link com.forge.storage.ConcurrentLsmKeyValueStore} for the
 * trigger policy and concurrency discipline.
 */
package com.forge.storage.compaction;
