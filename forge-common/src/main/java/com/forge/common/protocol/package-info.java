/**
 * FORGE's wire protocol: request/response message types and the binary
 * codec that frames them for transport over a TCP stream.
 *
 * <p>This package depends on no other FORGE module. It is used by both
 * {@code forge-server} and {@code forge-client} as the single shared
 * definition of what goes over the wire, so the two sides can never drift.
 */
package com.forge.common.protocol;
