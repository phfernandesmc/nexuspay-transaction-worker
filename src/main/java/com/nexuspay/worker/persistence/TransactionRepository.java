package com.nexuspay.worker.persistence;

import com.nexuspay.worker.domain.FailureReason;
import com.nexuspay.worker.domain.TransactionRecord;
import com.nexuspay.worker.domain.TransactionStatus;
import com.nexuspay.worker.domain.TransactionType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class TransactionRepository {

    private final JdbcClient jdbc;

    public TransactionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Le a transacao travando a linha ate o fim da transacao de banco.
     *
     * Medido (Task 8, round de correcao 1): quem realmente impede a
     * aplicacao dupla NAO e este FOR UPDATE — e a UNIQUE(transaction_id,
     * account_id) de ledger_entries, combinada com o SAVEPOINT em
     * TransactionProcessor.process. Removendo este FOR UPDATE e rodando
     * TransactionProcessorConcurrencyTest 5x, as 4 corridas continuaram
     * verdes nas 5 vezes: o segundo consumidor tambem chega a ler PENDING e
     * tambem tenta aplicar, mas o INSERT duplicado no ledger dispara
     * DuplicateKeyException, o savepoint desfaz o movimento parcial, e
     * process() retorna sem sobrescrever o status — mesmo resultado final.
     *
     * O que este FOR UPDATE de fato faz e mais barato que isso: serializa
     * os dois consumidores ANTES de qualquer UPDATE em accounts ou INSERT
     * em ledger_entries, entao o segundo bloqueia aqui, le o status ja
     * resolvido e sai sem gastar UPDATE/INSERT/rollback nenhum. E defesa em
     * profundidade e caminho rapido, nao a garantia — a garantia e a
     * constraint unica mais o rollback do savepoint.
     */
    public Optional<TransactionRecord> findForUpdate(UUID id) {
        return jdbc.sql("""
                SELECT id, type, source_account_id, destination_account_id, amount, status
                  FROM transactions
                 WHERE id = :id
                 FOR UPDATE
                """)
                .param("id", id)
                .query(TransactionRepository::mapear)
                .optional();
    }

    public void markCompleted(UUID id) {
        atualizarStatus(id, TransactionStatus.COMPLETED, null);
    }

    public void markFailed(UUID id, FailureReason reason) {
        atualizarStatus(id, TransactionStatus.FAILED, reason.name());
    }

    private void atualizarStatus(UUID id, TransactionStatus status, String motivo) {
        // CAST explicito: o driver JDBC envia o parametro como varchar, e o
        // PostgreSQL nao converte varchar para enum em atribuicao.
        jdbc.sql("""
                UPDATE transactions
                   SET status = CAST(:status AS transaction_status),
                       failure_reason = :motivo,
                       updated_at = now()
                 WHERE id = :id
                """)
                .param("status", status.name())
                .param("motivo", motivo)
                .param("id", id)
                .update();
    }

    private static TransactionRecord mapear(ResultSet rs, int linha) throws SQLException {
        return new TransactionRecord(
                rs.getObject("id", UUID.class),
                TransactionType.valueOf(rs.getString("type")),
                rs.getObject("source_account_id", UUID.class),
                rs.getObject("destination_account_id", UUID.class),
                rs.getBigDecimal("amount"),
                TransactionStatus.valueOf(rs.getString("status")));
    }
}
