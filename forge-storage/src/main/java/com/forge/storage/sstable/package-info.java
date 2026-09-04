/**
 * SSTables: immutable, sorted, on-disk snapshots produced by flushing a
 * {@link com.forge.storage.memtable.MemTable}. Written once (temp file,
 * fsync, atomic rename), never modified in place. See ARCHITECTURE.md
 * &sect;3.1 and DESIGN.md &sect;11 for the Phase 3 storage-engine contract.
 */
package com.forge.storage.sstable;
