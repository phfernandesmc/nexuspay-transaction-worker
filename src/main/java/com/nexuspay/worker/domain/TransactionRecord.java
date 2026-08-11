package com.nexuspay.worker.domain;

import java.math.BigDecimal;
import java.util.UUID;

/** Uma linha de transactions. sourceAccountId e nulo em deposito. */
public record TransactionRecord(
        UUID id,
        TransactionType type,
        UUID sourceAccountId,
        UUID destinationAccountId,
        BigDecimal amount,
        TransactionStatus status) {}
