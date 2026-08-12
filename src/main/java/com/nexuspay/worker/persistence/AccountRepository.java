package com.nexuspay.worker.persistence;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * O unico escritor de accounts.balance no sistema inteiro.
 *
 * Os dois metodos abaixo fazem a checagem e a escrita numa unica instrucao,
 * sem intervalo entre elas. Isso ELIMINA o check-then-act em vez de proteger
 * com trava — o formato de defeito que apareceu quatro vezes no gateway.
 * Zero linhas afetadas ja e a resposta de negocio; nao ha leitura anterior em
 * que confiar.
 */
@Repository
public class AccountRepository {

    private final JdbcClient jdbc;

    public AccountRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Saldo resultante, ou vazio se a conta nao esta ativa ou nao tem saldo. */
    public Optional<BigDecimal> debit(UUID accountId, BigDecimal amount) {
        return jdbc.sql("""
                UPDATE accounts
                   SET balance = balance - :valor, updated_at = now()
                 WHERE id = :id AND status = 'ACTIVE' AND balance >= :valor
                RETURNING balance
                """)
                .param("valor", amount)
                .param("id", accountId)
                .query(BigDecimal.class)
                .optional();
    }

    /** Saldo resultante, ou vazio se a conta nao esta ativa ou nao existe. */
    public Optional<BigDecimal> credit(UUID accountId, BigDecimal amount) {
        return jdbc.sql("""
                UPDATE accounts
                   SET balance = balance + :valor, updated_at = now()
                 WHERE id = :id AND status = 'ACTIVE'
                RETURNING balance
                """)
                .param("valor", amount)
                .param("id", accountId)
                .query(BigDecimal.class)
                .optional();
    }

    /**
     * Diagnostico, nunca decisao: serve so para escolher entre
     * INSUFFICIENT_FUNDS e *_ACCOUNT_UNAVAILABLE depois que o UPDATE ja
     * decidiu, devolvendo zero linhas.
     */
    public boolean isActive(UUID accountId) {
        return jdbc.sql("SELECT 1 FROM accounts WHERE id = :id AND status = 'ACTIVE'")
                .param("id", accountId)
                .query(Integer.class)
                .optional()
                .isPresent();
    }
}
