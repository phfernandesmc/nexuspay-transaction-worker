package com.nexuspay.worker;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * O schema vive no repositorio do gateway e chega aqui como dump versionado.
 * Este teste existe para que uma migration futura que renomeie ou remova uma
 * coluna de que o worker depende apareca como falha de teste, e nao como
 * excecao em producao.
 *
 * Quando ele falhar: rode scripts/regenerate-schema.sh e ajuste o worker.
 */
class SchemaDriftTest extends PostgresTestBase {

    @Autowired
    JdbcClient jdbc;

    private List<String> colunas(String tabela) {
        return jdbc.sql("""
                SELECT column_name FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = :tabela
                """)
                .param("tabela", tabela)
                .query(String.class)
                .list();
    }

    @Test
    void transactions_tem_as_colunas_que_o_worker_usa() {
        assertThat(colunas("transactions")).contains(
                "id", "type", "source_account_id", "destination_account_id",
                "amount", "status", "failure_reason", "updated_at");
    }

    @Test
    void accounts_tem_as_colunas_que_o_worker_usa() {
        assertThat(colunas("accounts")).contains("id", "balance", "status", "updated_at");
    }

    @Test
    void ledger_entries_tem_as_colunas_que_o_worker_grava() {
        assertThat(colunas("ledger_entries")).contains(
                "id", "transaction_id", "account_id", "direction",
                "amount", "balance_after", "created_at");
    }

    @Test
    void a_unica_do_ledger_existe() {
        var constraints = jdbc.sql("""
                SELECT conname FROM pg_constraint
                 WHERE conrelid = 'ledger_entries'::regclass AND contype = 'u'
                """).query(String.class).list();

        assertThat(constraints).contains("uq_ledger_transaction_account");
    }

    @Test
    void o_check_de_saldo_nao_negativo_das_contas_existe() {
        var constraints = jdbc.sql("""
                SELECT conname FROM pg_constraint
                 WHERE conrelid = 'accounts'::regclass AND contype = 'c'
                """).query(String.class).list();

        assertThat(constraints).contains("check_positive_balance");
    }
}
