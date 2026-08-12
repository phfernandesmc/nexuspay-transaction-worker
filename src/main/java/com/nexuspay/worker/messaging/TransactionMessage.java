package com.nexuspay.worker.messaging;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * Corpo publicado pelo gateway: {"transaction_id": "<uuid>"}.
 *
 * O nome do campo e snake_case do lado Python. @JsonProperty explicito em vez
 * de estrategia global de nomes: e um contrato entre servicos, e deixa-lo
 * depender de configuracao global do Jackson faria uma mudanca nao relacionada
 * quebrar o consumo.
 */
public record TransactionMessage(@JsonProperty("transaction_id") UUID transactionId) {}
