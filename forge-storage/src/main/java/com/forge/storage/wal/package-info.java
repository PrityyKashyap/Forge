/**
 * The write-ahead log: durable, crash-safe, checksummed recording of every
 * mutation before it is allowed to become visible in servable state. See
 * ARCHITECTURE.md &sect;3.1 and DESIGN.md &sect;11 (WAL per-feature contract).
 */
package com.forge.storage.wal;
