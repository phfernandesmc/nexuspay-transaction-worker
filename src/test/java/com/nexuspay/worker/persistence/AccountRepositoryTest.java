package com.nexuspay.worker.persistence;

import com.nexuspay.worker.Fixtures;
import com.nexuspay.worker.PostgresTestBase;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
class AccountRepositoryTest extends PostgresTestBase {

    @Autowired
    JdbcClient jdbc;

    @Autowired
    AccountRepository repository;

    private BigDecimal saldo(UUID conta) {
        return jdbc.sql("SELECT balance FROM accounts WHERE id = :id")
                .param("id", conta).query(BigDecimal.class).single();
    }

    @Test
    void debito_com_saldo_suficiente_devolve_o_saldo_resultante() {
        var conta = Fixtures.criarConta(jdbc, "500.00");

        var depois = repository.debit(conta, new BigDecimal("100.00"));

        assertThat(depois).isPresent();
        assertThat(depois.get()).isEqualByComparingTo(new BigDecimal("400.00"));
        assertThat(saldo(conta)).isEqualByComparingTo(new BigDecimal("400.00"));
    }

    @Test
    void debito_exatamente_igual_ao_saldo_e_permitido() {
        var conta = Fixtures.criarConta(jdbc, "100.00");

        var depois = repository.debit(conta, new BigDecimal("100.00"));

        assertThat(depois.orElseThrow()).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void debito_maior_que_o_saldo_nao_aplica_e_nao_mexe_no_saldo() {
        var conta = Fixtures.criarConta(jdbc, "50.00");

        var depois = repository.debit(conta, new BigDecimal("100.00"));

        assertThat(depois).isEmpty();
        assertThat(saldo(conta)).isEqualByComparingTo(new BigDecimal("50.00"));
    }

    @Test
    void debito_em_conta_encerrada_nao_aplica() {
        var conta = Fixtures.criarConta(jdbc, "500.00");
        jdbc.sql("UPDATE accounts SET status = 'CLOSED' WHERE id = :id")
                .param("id", conta).update();

        assertThat(repository.debit(conta, new BigDecimal("10.00"))).isEmpty();
        assertThat(saldo(conta)).isEqualByComparingTo(new BigDecimal("500.00"));
    }

    @Test
    void credito_em_conta_ativa_devolve_o_saldo_resultante() {
        var conta = Fixtures.criarConta(jdbc, "10.00");

        var depois = repository.credit(conta, new BigDecimal("90.00"));

        assertThat(depois.orElseThrow()).isEqualByComparingTo(new BigDecimal("100.00"));
    }

    @Test
    void credito_em_conta_encerrada_nao_aplica() {
        var conta = Fixtures.criarConta(jdbc, "0.00");
        jdbc.sql("UPDATE accounts SET status = 'CLOSED' WHERE id = :id")
                .param("id", conta).update();

        assertThat(repository.credit(conta, new BigDecimal("10.00"))).isEmpty();
        assertThat(saldo(conta)).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void conta_inexistente_nao_aplica_em_nenhum_dos_dois_sentidos() {
        var fantasma = UUID.randomUUID();

        assertThat(repository.debit(fantasma, new BigDecimal("1.00"))).isEmpty();
        assertThat(repository.credit(fantasma, new BigDecimal("1.00"))).isEmpty();
    }

    @Test
    void is_active_distingue_conta_ativa_de_encerrada_e_de_inexistente() {
        var ativa = Fixtures.criarConta(jdbc, "0.00");
        var encerrada = Fixtures.criarConta(jdbc, "0.00");
        jdbc.sql("UPDATE accounts SET status = 'CLOSED' WHERE id = :id")
                .param("id", encerrada).update();

        assertThat(repository.isActive(ativa)).isTrue();
        assertThat(repository.isActive(encerrada)).isFalse();
        assertThat(repository.isActive(UUID.randomUUID())).isFalse();
    }
}
