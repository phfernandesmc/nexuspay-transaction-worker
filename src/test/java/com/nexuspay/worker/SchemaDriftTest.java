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

    private List<String> valoresDoEnum(String tipo) {
        // enum_range devolve os rotulos na ordem de DECLARACAO, que e tambem a
        // ordem que o Postgres usa em ORDER BY sobre a coluna.
        return jdbc.sql("SELECT unnest(enum_range(NULL::" + tipo + "))::text")
                .query(String.class)
                .list();
    }

    /**
     * Presenca de coluna nao basta: o worker faz valueOf() sobre o TEXTO de
     * transaction_status e transaction_type. Um valor novo acrescentado pelo
     * gateway (um REVERSED, um SCHEDULED) chega aqui como
     * IllegalArgumentException dentro do mapeamento da linha — ANTES do guarda
     * de status, e portanto sem ser falha de negocio. A mensagem nunca e
     * deletada, reentrega 10 vezes ate a DLQ e trava o MessageGroupId (a conta
     * de origem) por 18 a 20 minutos, com este teste de divergencia verde o
     * tempo todo. E o que estes tres testes fecham.
     *
     * containsExactly, nao containsExactlyInAnyOrder: a ORDEM tambem e
     * contrato. ledger_direction ordenado por declaracao e o que faz
     * "ORDER BY direction" significar DEBIT antes de CREDIT.
     */
    @Test
    void transaction_status_tem_exatamente_os_valores_que_o_worker_conhece() {
        assertThat(valoresDoEnum("transaction_status"))
                .containsExactly("PENDING", "COMPLETED", "FAILED");
    }

    @Test
    void transaction_type_tem_exatamente_os_valores_que_o_worker_conhece() {
        assertThat(valoresDoEnum("transaction_type"))
                .containsExactly("DEPOSIT", "TRANSFER");
    }

    @Test
    void ledger_direction_tem_exatamente_os_valores_que_o_worker_grava() {
        assertThat(valoresDoEnum("ledger_direction"))
                .containsExactly("DEBIT", "CREDIT");
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

    /**
     * AccountRepository.debit e credit nao validam que amount e positivo, e
     * nao precisam: o valor vem da coluna transactions.amount, que o banco
     * garante ser maior que zero. Se este CHECK sumir, um amount negativo
     * transforma um credito em debito silencioso, sem passar por nenhuma
     * validacao do worker.
     */
    @Test
    void o_check_de_amount_positivo_das_transacoes_existe() {
        var constraints = jdbc.sql("""
                SELECT conname FROM pg_constraint
                 WHERE conrelid = 'transactions'::regclass AND contype = 'c'
                """).query(String.class).list();

        assertThat(constraints).contains("check_positive_amount");
    }
}
