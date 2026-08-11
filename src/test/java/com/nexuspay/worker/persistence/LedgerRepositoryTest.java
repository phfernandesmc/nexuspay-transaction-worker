package com.nexuspay.worker.persistence;

import com.nexuspay.worker.Fixtures;
import com.nexuspay.worker.PostgresTestBase;
import com.nexuspay.worker.domain.LedgerDirection;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Transactional
class LedgerRepositoryTest extends PostgresTestBase {

    @Autowired
    JdbcClient jdbc;

    @Autowired
    LedgerRepository repository;

    @Test
    void grava_o_lancamento_com_sentido_e_saldo_resultante() {
        var conta = Fixtures.criarConta(jdbc, "400.00");
        var tx = Fixtures.criarDeposito(jdbc, conta, "100.00");

        repository.insert(tx, conta, LedgerDirection.CREDIT,
                new BigDecimal("100.00"), new BigDecimal("400.00"));

        var sentido = jdbc.sql("""
                SELECT direction FROM ledger_entries
                 WHERE transaction_id = :tx AND account_id = :conta
                """)
                .param("tx", tx).param("conta", conta)
                .query(String.class).single();
        assertThat(sentido).isEqualTo("CREDIT");
    }

    @Test
    void a_mesma_transacao_pode_lancar_nas_duas_contas() {
        var origem = Fixtures.criarConta(jdbc, "400.00");
        var destino = Fixtures.criarConta(jdbc, "100.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        repository.insert(tx, origem, LedgerDirection.DEBIT,
                new BigDecimal("100.00"), new BigDecimal("400.00"));
        repository.insert(tx, destino, LedgerDirection.CREDIT,
                new BigDecimal("100.00"), new BigDecimal("100.00"));

        var total = jdbc.sql("SELECT count(*) FROM ledger_entries WHERE transaction_id = :tx")
                .param("tx", tx).query(Integer.class).single();
        assertThat(total).isEqualTo(2);
    }

    @Test
    void lancar_duas_vezes_na_mesma_conta_estoura_a_unica() {
        // Esta e a ultima rede contra aplicacao dupla. Se o SELECT FOR UPDATE
        // e a checagem de status falharem, e ela que impede o saldo de andar
        // duas vezes.
        var conta = Fixtures.criarConta(jdbc, "100.00");
        var tx = Fixtures.criarDeposito(jdbc, conta, "100.00");
        repository.insert(tx, conta, LedgerDirection.CREDIT,
                new BigDecimal("100.00"), new BigDecimal("100.00"));

        assertThatThrownBy(() -> repository.insert(tx, conta, LedgerDirection.CREDIT,
                new BigDecimal("100.00"), new BigDecimal("200.00")))
                .isInstanceOf(DuplicateKeyException.class);
    }
}
