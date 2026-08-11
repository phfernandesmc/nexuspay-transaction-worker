package com.nexuspay.worker.domain;

/**
 * Conjunto fechado de codigos, nao texto livre: o frontend da fatia 3 traduz
 * por codigo, do mesmo jeito que ja faz com o envelope de erro do gateway.
 * Cabe em failure_reason VARCHAR(255).
 */
public enum FailureReason {
    INSUFFICIENT_FUNDS,
    SOURCE_ACCOUNT_UNAVAILABLE,
    DESTINATION_ACCOUNT_UNAVAILABLE
}
