package com.nexuspay.worker.service;

import com.nexuspay.worker.Fixtures;
import com.nexuspay.worker.PostgresTestBase;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concorrencia real, com duas conexoes fisicas. Sem @Transactional: cada
 * thread precisa da propria transacao de banco, e um teste transacional em
 * volta serializaria tudo numa conexao so, escondendo exatamente o que
 * queremos provar.
 */
class TransactionProcessorConcurrencyTest extends PostgresTestBase {

    @Autowired
    JdbcClient jdbc;

    @Autowired
    TransactionProcessor processor;

    private BigDecimal saldo(UUID conta) {
        return jdbc.sql("SELECT balance FROM accounts WHERE id = :id")
                .param("id", conta).query(BigDecimal.class).single();
    }

    private List<String> statusDe(UUID... ids) {
        return List.of(ids).stream()
                .map(id -> jdbc.sql("SELECT status FROM transactions WHERE id = :id")
                        .param("id", id).query(String.class).single())
                .toList();
    }

    private void emParalelo(Runnable a, Runnable b) throws Exception {
        try (var pool = Executors.newFixedThreadPool(2)) {
            List<Callable<Void>> tarefas = List.of(
                    () -> { a.run(); return null; },
                    () -> { b.run(); return null; });
            for (var futuro : pool.invokeAll(tarefas)) {
                futuro.get();  // propaga qualquer excecao
            }
        }
    }

    @Test
    void duas_transferencias_de_100_contra_saldo_100_so_uma_passa() throws Exception {
        // O buraco que o gateway deixa aberto de proposito: as duas recebem 202
        // la, e o worker e quem tem que ser a autoridade.
        var origem = Fixtures.criarConta(jdbc, "100.00");
        var destinoA = Fixtures.criarConta(jdbc, "0.00");
        var destinoB = Fixtures.criarConta(jdbc, "0.00");
        var txA = Fixtures.criarTransferencia(jdbc, origem, destinoA, "100.00");
        var txB = Fixtures.criarTransferencia(jdbc, origem, destinoB, "100.00");

        emParalelo(() -> processor.process(txA), () -> processor.process(txB));

        assertThat(statusDe(txA, txB))
                .containsExactlyInAnyOrder("COMPLETED", "FAILED");
        assertThat(saldo(origem)).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void transferencias_inversas_concorrentes_concluem_sem_deadlock() throws Exception {
        var contaA = Fixtures.criarConta(jdbc, "500.00");
        var contaB = Fixtures.criarConta(jdbc, "500.00");
        var aParaB = Fixtures.criarTransferencia(jdbc, contaA, contaB, "100.00");
        var bParaA = Fixtures.criarTransferencia(jdbc, contaB, contaA, "100.00");

        emParalelo(() -> processor.process(aParaB), () -> processor.process(bParaA));

        assertThat(statusDe(aParaB, bParaA)).containsExactly("COMPLETED", "COMPLETED");
        assertThat(saldo(contaA)).isEqualByComparingTo(new BigDecimal("500.00"));
        assertThat(saldo(contaB)).isEqualByComparingTo(new BigDecimal("500.00"));
    }

    @Test
    void a_mesma_transacao_processada_em_paralelo_aplica_uma_vez_so() throws Exception {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        emParalelo(() -> processor.process(tx), () -> processor.process(tx));

        assertThat(saldo(origem)).isEqualByComparingTo(new BigDecimal("400.00"));
        assertThat(saldo(destino)).isEqualByComparingTo(new BigDecimal("100.00"));
        assertThat(jdbc.sql("SELECT count(*) FROM ledger_entries WHERE transaction_id = :id")
                .param("id", tx).query(Integer.class).single()).isEqualTo(2);
    }

    @Test
    void o_saldo_nunca_fica_negativo_sob_disputa() throws Exception {
        var origem = Fixtures.criarConta(jdbc, "150.00");
        var d1 = Fixtures.criarConta(jdbc, "0.00");
        var d2 = Fixtures.criarConta(jdbc, "0.00");
        var t1 = Fixtures.criarTransferencia(jdbc, origem, d1, "100.00");
        var t2 = Fixtures.criarTransferencia(jdbc, origem, d2, "100.00");

        emParalelo(() -> processor.process(t1), () -> processor.process(t2));

        assertThat(saldo(origem)).isGreaterThanOrEqualTo(BigDecimal.ZERO);
        assertThat(saldo(origem)).isEqualByComparingTo(new BigDecimal("50.00"));
    }
}
