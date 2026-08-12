package com.nexuspay.worker.service;

import com.nexuspay.worker.Fixtures;
import com.nexuspay.worker.PostgresTestBase;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sem @Transactional na classe: o processador abre a propria transacao, e um
 * teste transacional em volta transformaria o SAVEPOINT interno em algo
 * diferente do que roda em producao. Cada teste cria dados novos e nao limpa —
 * o container e descartado no fim da suite.
 */
class TransactionProcessorTest extends PostgresTestBase {

    @Autowired
    JdbcClient jdbc;

    @Autowired
    TransactionProcessor processor;

    private BigDecimal saldo(UUID conta) {
        return jdbc.sql("SELECT balance FROM accounts WHERE id = :id")
                .param("id", conta).query(BigDecimal.class).single();
    }

    private String status(UUID tx) {
        return jdbc.sql("SELECT status FROM transactions WHERE id = :id")
                .param("id", tx).query(String.class).single();
    }

    private String motivo(UUID tx) {
        return jdbc.sql("SELECT failure_reason FROM transactions WHERE id = :id")
                .param("id", tx).query(String.class).optional().orElse(null);
    }

    private int lancamentos(UUID tx) {
        return jdbc.sql("SELECT count(*) FROM ledger_entries WHERE transaction_id = :id")
                .param("id", tx).query(Integer.class).single();
    }

    @Test
    void transferencia_valida_move_saldo_e_grava_dois_lancamentos() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "100.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        processor.process(tx);

        assertThat(status(tx)).isEqualTo("COMPLETED");
        assertThat(saldo(origem)).isEqualByComparingTo(new BigDecimal("400.00"));
        assertThat(saldo(destino)).isEqualByComparingTo(new BigDecimal("200.00"));
        assertThat(lancamentos(tx)).isEqualTo(2);
    }

    @Test
    void deposito_valido_credita_e_grava_um_lancamento() {
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarDeposito(jdbc, destino, "250.00");

        processor.process(tx);

        assertThat(status(tx)).isEqualTo("COMPLETED");
        assertThat(saldo(destino)).isEqualByComparingTo(new BigDecimal("250.00"));
        assertThat(lancamentos(tx)).isEqualTo(1);
    }

    @Test
    void saldo_insuficiente_termina_em_failed_sem_mexer_em_nada() {
        var origem = Fixtures.criarConta(jdbc, "50.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        processor.process(tx);

        assertThat(status(tx)).isEqualTo("FAILED");
        assertThat(motivo(tx)).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(saldo(origem)).isEqualByComparingTo(new BigDecimal("50.00"));
        assertThat(saldo(destino)).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(lancamentos(tx)).isZero();
    }

    @Test
    void destino_encerrado_termina_em_failed() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");
        jdbc.sql("UPDATE accounts SET status = 'CLOSED' WHERE id = :id")
                .param("id", destino).update();

        processor.process(tx);

        assertThat(status(tx)).isEqualTo("FAILED");
        assertThat(motivo(tx)).isEqualTo("DESTINATION_ACCOUNT_UNAVAILABLE");
        assertThat(saldo(origem)).isEqualByComparingTo(new BigDecimal("500.00"));
    }

    @Test
    void origem_encerrada_termina_em_failed_com_motivo_proprio() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");
        jdbc.sql("UPDATE accounts SET status = 'CLOSED' WHERE id = :id")
                .param("id", origem).update();

        processor.process(tx);

        assertThat(status(tx)).isEqualTo("FAILED");
        assertThat(motivo(tx)).isEqualTo("SOURCE_ACCOUNT_UNAVAILABLE");
        assertThat(saldo(destino)).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void credito_aplicado_antes_do_debito_e_desfeito_quando_o_debito_falha() {
        // O teste do SAVEPOINT. Os UPDATE saem em ordem crescente de UUID, entao
        // com o destino de UUID menor o CREDITO acontece PRIMEIRO. Quando o
        // debito devolve zero linhas, o credito ja aplicado precisa sumir — e o
        // FAILED precisa sobreviver. Sem SAVEPOINT, ou o destino fica creditado
        // numa transferencia que falhou, ou a transacao volta para PENDING e a
        // varredura do gateway a republica para sempre.
        UUID destino;
        UUID origem;
        do {
            destino = Fixtures.criarConta(jdbc, "0.00");
            origem = Fixtures.criarConta(jdbc, "50.00");
        } while (destino.compareTo(origem) >= 0);

        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        processor.process(tx);

        assertThat(status(tx)).isEqualTo("FAILED");
        assertThat(motivo(tx)).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(saldo(destino))
                .as("o credito adiantado tem que ter sido desfeito")
                .isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(lancamentos(tx)).isZero();
    }

    @Test
    void uuid_inexistente_nao_faz_nada_e_nao_levanta() {
        processor.process(UUID.randomUUID());
    }

    @Test
    void transacao_ja_concluida_nao_e_aplicada_de_novo() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        processor.process(tx);
        processor.process(tx);

        assertThat(saldo(origem)).isEqualByComparingTo(new BigDecimal("400.00"));
        assertThat(saldo(destino)).isEqualByComparingTo(new BigDecimal("100.00"));
        assertThat(lancamentos(tx)).isEqualTo(2);
    }

    @Test
    void os_lancamentos_registram_o_saldo_resultante_de_cada_lado() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "100.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        processor.process(tx);

        List<BigDecimal> saldos = jdbc.sql("""
                SELECT balance_after FROM ledger_entries
                 WHERE transaction_id = :tx ORDER BY direction
                """).param("tx", tx).query(BigDecimal.class).list();

        // ORDER BY direction: enum ledger_direction e declarado no schema como
        // ('DEBIT', 'CREDIT') (schema.sql linha 44-47) — Postgres ordena enum
        // pela ordem de DECLARACAO, nao alfabetica. DEBIT sai primeiro.
        // Desvio do brief: o comentario original supunha ordem alfabetica
        // (CREDIT antes de DEBIT), o que so e verdade em texto puro, nao para
        // este tipo enumerado; os valores esperados aqui foram trocados.
        assertThat(saldos.get(0)).isEqualByComparingTo(new BigDecimal("400.00"));
        assertThat(saldos.get(1)).isEqualByComparingTo(new BigDecimal("200.00"));
    }
}
