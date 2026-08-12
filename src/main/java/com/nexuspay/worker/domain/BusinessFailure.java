package com.nexuspay.worker.domain;

/**
 * Falha de regra, nao de infraestrutura.
 *
 * A distincao decide o destino da mensagem: falha de negocio termina a
 * transacao em FAILED e a mensagem e deletada; falha de infraestrutura sobe,
 * a mensagem nao e deletada e a SQS reentrega.
 */
public class BusinessFailure extends RuntimeException {

    private final FailureReason reason;

    public BusinessFailure(FailureReason reason) {
        super(reason.name());
        this.reason = reason;
    }

    public FailureReason reason() {
        return reason;
    }
}
