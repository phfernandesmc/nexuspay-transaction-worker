package com.nexuspay.worker.messaging;

import com.nexuspay.worker.service.TransactionProcessor;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Ponto de entrada unico do worker. Nao ha API de negocio.
 *
 * O modo de confirmacao padrao (ON_SUCCESS) e exatamente a semantica que o
 * spec pede e por isso nao e sobrescrito: retorno normal deleta a mensagem,
 * excecao NAO deleta e a SQS reentrega ate a redrive policy mandar para a DLQ.
 * Todo caminho terminal — inclusive falha de negocio e UUID desconhecido —
 * retorna normalmente de proposito.
 *
 * Condicional em propriedade porque os testes de banco herdam de
 * PostgresTestBase e subiriam um consumidor apontado para uma URL invalida.
 * Ligado por padrao (matchIfMissing): producao nao precisa configurar nada, e
 * so o ambiente de teste desliga.
 */
@Component
@ConditionalOnProperty(name = "nexuspay.sqs.listener-enabled",
        havingValue = "true", matchIfMissing = true)
class TransactionListener {

    private final TransactionProcessor processor;

    TransactionListener(TransactionProcessor processor) {
        this.processor = processor;
    }

    @SqsListener("${nexuspay.sqs.queue-url}")
    void aoReceber(TransactionMessage mensagem) {
        processor.process(mensagem.transactionId());
    }
}
