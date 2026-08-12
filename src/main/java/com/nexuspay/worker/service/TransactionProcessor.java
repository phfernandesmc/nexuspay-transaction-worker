package com.nexuspay.worker.service;

import com.nexuspay.worker.domain.BusinessFailure;
import com.nexuspay.worker.domain.FailureReason;
import com.nexuspay.worker.domain.LedgerDirection;
import com.nexuspay.worker.domain.TransactionRecord;
import com.nexuspay.worker.domain.TransactionStatus;
import com.nexuspay.worker.domain.TransactionType;
import com.nexuspay.worker.persistence.AccountRepository;
import com.nexuspay.worker.persistence.LedgerRepository;
import com.nexuspay.worker.persistence.TransactionRepository;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class TransactionProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransactionProcessor.class);

    private final TransactionRepository transactions;
    private final AccountRepository accounts;
    private final LedgerRepository ledger;
    private final TransactionTemplate nested;

    public TransactionProcessor(TransactionRepository transactions,
                                AccountRepository accounts,
                                LedgerRepository ledger,
                                TransactionTemplate nestedTransactionTemplate) {
        this.transactions = transactions;
        this.accounts = accounts;
        this.ledger = ledger;
        this.nested = nestedTransactionTemplate;
    }

    /** Um movimento de conta a aplicar, com o sentido. */
    private record Perna(UUID accountId, LedgerDirection direction) {}

    @Transactional
    public void process(UUID transactionId) {
        var encontrada = transactions.findForUpdate(transactionId);
        if (encontrada.isEmpty()) {
            // No-op TERMINAL. A fila e compartilhada entre ambientes e a
            // mensagem carrega so o UUID: um worker que nao acha a linha
            // recebeu mensagem de outro ambiente. Insistir faria a mensagem
            // voltar para sempre.
            log.info("transacao {} nao existe neste banco; descartando a mensagem",
                    transactionId);
            return;
        }

        var transacao = encontrada.get();
        if (transacao.status() != TransactionStatus.PENDING) {
            log.info("transacao {} ja esta {}; reentrega descartada",
                    transactionId, transacao.status());
            return;
        }

        try {
            nested.executeWithoutResult(status -> aplicar(transacao));
        } catch (BusinessFailure falha) {
            // O SAVEPOINT ja desfez qualquer movimento parcial; o FAILED
            // abaixo esta fora dele e sobrevive ao commit.
            log.info("transacao {} recusada: {}", transactionId, falha.reason());
            transactions.markFailed(transactionId, falha.reason());
            return;
        } catch (DuplicateKeyException duplicada) {
            // A unica do ledger disparou: outro consumidor ja aplicou esta
            // transacao. O status dele e a verdade; nao sobrescrevemos.
            log.warn("transacao {} ja possuia lancamento; nada aplicado", transactionId);
            return;
        }

        transactions.markCompleted(transactionId);
        log.info("transacao {} concluida", transactionId);
    }

    private void aplicar(TransactionRecord transacao) {
        if (transacao.type() == TransactionType.DEPOSIT) {
            aplicarPerna(transacao,
                    new Perna(transacao.destinationAccountId(), LedgerDirection.CREDIT));
            return;
        }

        // Ordem crescente de UUID: MessageGroupId e a conta de ORIGEM, entao
        // A->B e B->A caem em grupos diferentes, rodam em paralelo e tocam as
        // mesmas duas linhas. Sem ordenacao fixa, isso e deadlock.
        List<Perna> pernas = List.of(
                        new Perna(transacao.sourceAccountId(), LedgerDirection.DEBIT),
                        new Perna(transacao.destinationAccountId(), LedgerDirection.CREDIT))
                .stream()
                .sorted(Comparator.comparing(Perna::accountId))
                .toList();

        for (var perna : pernas) {
            aplicarPerna(transacao, perna);
        }
    }

    private void aplicarPerna(TransactionRecord transacao, Perna perna) {
        var resultado = perna.direction() == LedgerDirection.DEBIT
                ? accounts.debit(perna.accountId(), transacao.amount())
                : accounts.credit(perna.accountId(), transacao.amount());

        BigDecimal saldoDepois = resultado.orElseThrow(() -> new BusinessFailure(motivo(perna)));
        ledger.insert(transacao.id(), perna.accountId(), perna.direction(),
                transacao.amount(), saldoDepois);
    }

    /**
     * Diagnostico, nunca decisao: a decisao ja foi tomada pelo UPDATE, que
     * devolveu zero linhas. Isto so escolhe qual codigo gravar.
     */
    private FailureReason motivo(Perna perna) {
        if (perna.direction() == LedgerDirection.CREDIT) {
            return FailureReason.DESTINATION_ACCOUNT_UNAVAILABLE;
        }
        return accounts.isActive(perna.accountId())
                ? FailureReason.INSUFFICIENT_FUNDS
                : FailureReason.SOURCE_ACCOUNT_UNAVAILABLE;
    }
}
