package com.nexuspay.worker.persistence;

import com.nexuspay.worker.domain.LedgerDirection;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Auditoria do movimento de saldo.
 *
 * balance_after e o que torna o saldo verificavel: ele passa a ser
 * reconstruivel a partir do historico, e divergencia entre accounts.balance e
 * o ultimo balance_after vira detectavel.
 */
@Repository
public class LedgerRepository {

    private final JdbcClient jdbc;

    public LedgerRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(UUID transactionId, UUID accountId, LedgerDirection direction,
                       BigDecimal amount, BigDecimal balanceAfter) {
        jdbc.sql("""
                INSERT INTO ledger_entries
                  (id, transaction_id, account_id, direction, amount, balance_after)
                VALUES
                  (:id, :tx, :conta, CAST(:sentido AS ledger_direction), :valor, :depois)
                """)
                .param("id", UUID.randomUUID())
                .param("tx", transactionId)
                .param("conta", accountId)
                .param("sentido", direction.name())
                .param("valor", amount)
                .param("depois", balanceAfter)
                .update();
    }
}
