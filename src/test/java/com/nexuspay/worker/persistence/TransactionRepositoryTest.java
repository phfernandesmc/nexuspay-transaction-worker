package com.nexuspay.worker.persistence;

import com.nexuspay.worker.Fixtures;
import com.nexuspay.worker.PostgresTestBase;
import com.nexuspay.worker.domain.FailureReason;
import com.nexuspay.worker.domain.TransactionStatus;
import com.nexuspay.worker.domain.TransactionType;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
class TransactionRepositoryTest extends PostgresTestBase {

    @Autowired
    JdbcClient jdbc;

    @Autowired
    TransactionRepository repository;

    @Test
    void le_uma_transferencia_com_os_dois_lados() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var id = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        var encontrada = repository.findForUpdate(id).orElseThrow();

        assertThat(encontrada.type()).isEqualTo(TransactionType.TRANSFER);
        assertThat(encontrada.sourceAccountId()).isEqualTo(origem);
        assertThat(encontrada.destinationAccountId()).isEqualTo(destino);
        assertThat(encontrada.amount()).isEqualByComparingTo(new BigDecimal("100.00"));
        assertThat(encontrada.status()).isEqualTo(TransactionStatus.PENDING);
    }

    @Test
    void deposito_vem_com_origem_nula() {
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var id = Fixtures.criarDeposito(jdbc, destino, "100.00");

        var encontrada = repository.findForUpdate(id).orElseThrow();

        assertThat(encontrada.type()).isEqualTo(TransactionType.DEPOSIT);
        assertThat(encontrada.sourceAccountId()).isNull();
    }

    @Test
    void uuid_inexistente_devolve_vazio() {
        assertThat(repository.findForUpdate(UUID.randomUUID())).isEmpty();
    }

    @Test
    void marcar_concluida_grava_status_e_updated_at() {
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var id = Fixtures.criarDeposito(jdbc, destino, "100.00");

        repository.markCompleted(id);

        assertThat(repository.findForUpdate(id).orElseThrow().status())
                .isEqualTo(TransactionStatus.COMPLETED);
        assertThat(jdbc.sql("SELECT updated_at FROM transactions WHERE id = :id")
                .param("id", id).query(java.time.OffsetDateTime.class).optional())
                .isPresent();
    }

    @Test
    void marcar_falha_grava_status_e_motivo() {
        var origem = Fixtures.criarConta(jdbc, "0.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var id = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        repository.markFailed(id, FailureReason.INSUFFICIENT_FUNDS);

        assertThat(repository.findForUpdate(id).orElseThrow().status())
                .isEqualTo(TransactionStatus.FAILED);
        assertThat(jdbc.sql("SELECT failure_reason FROM transactions WHERE id = :id")
                .param("id", id).query(String.class).single())
                .isEqualTo("INSUFFICIENT_FUNDS");
    }
}
