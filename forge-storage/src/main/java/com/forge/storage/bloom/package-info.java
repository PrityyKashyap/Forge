/**
 * Bloom filters used to skip a doomed SSTable scan for a key that
 * definitely isn't present — Phase 13. See {@link com.forge.storage.bloom.BloomFilter}
 * for the no-false-negatives safety property this is built on, and
 * {@link com.forge.storage.bloom.BloomFilterFile} for why a missing or
 * corrupt filter is never treated as fatal.
 */
package com.forge.storage.bloom;
