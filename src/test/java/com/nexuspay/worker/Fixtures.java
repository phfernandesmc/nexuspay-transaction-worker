package com.nexuspay.worker;

import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Semeia dados diretamente por SQL. O worker nao tem caminho de criacao de
 * conta nem de transacao — quem cria e o gateway, que e outro processo.
 */
public final class Fixtures {

    private Fixtures() {}

    public static UUID criarConta(JdbcClient jdbc, String saldo) {
        var institutionId = jdbc.sql("SELECT id FROM institutions LIMIT 1")
                .query(UUID.class).optional()
                .orElseGet(() -> criarInstituicao(jdbc));

        var ownerId = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO users (id, full_name, email, document, password_hash)
                VALUES (:id, 'Dono De Teste', :email, :doc, 'x')
                """)
                .param("id", ownerId)
                .param("email", "dono-" + ownerId + "@example.com")
                .param("doc", String.valueOf(Math.abs(ownerId.getLeastSignificantBits())).substring(0, 11))
                .update();

        var accountId = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO accounts (id, owner_id, institution_id, branch, number, type, balance)
                VALUES (:id, :owner, :inst, '0001', :num, 'CHECKING', :saldo)
                """)
                .param("id", accountId)
                .param("owner", ownerId)
                .param("inst", institutionId)
                .param("num", accountId.toString().substring(0, 8))
                .param("saldo", new BigDecimal(saldo))
                .update();
        return accountId;
    }

    private static UUID criarInstituicao(JdbcClient jdbc) {
        var id = UUID.randomUUID();
        // Desvio do brief: institutions.color_hex e CHAR(7) NOT NULL sem
        // default no schema real (src/test/resources/schema.sql linha 127).
        // O INSERT verbatim do brief (id, code, name) falha com violacao de
        // NOT NULL; adicionado color_hex aqui.
        jdbc.sql("""
                INSERT INTO institutions (id, code, name, color_hex)
                VALUES (:id, '001', 'Banco De Teste', '#000000')
                """)
                .param("id", id).update();
        return id;
    }

    public static UUID criarTransferencia(JdbcClient jdbc, UUID origem, UUID destino, String valor) {
        return inserirTransacao(jdbc, "TRANSFER", origem, destino, valor);
    }

    public static UUID criarDeposito(JdbcClient jdbc, UUID destino, String valor) {
        return inserirTransacao(jdbc, "DEPOSIT", null, destino, valor);
    }

    private static UUID inserirTransacao(
            JdbcClient jdbc, String tipo, UUID origem, UUID destino, String valor) {
        var id = UUID.randomUUID();
        var requester = jdbc.sql("SELECT owner_id FROM accounts WHERE id = :id")
                .param("id", destino).query(UUID.class).single();
        jdbc.sql("""
                INSERT INTO transactions
                  (id, type, source_account_id, destination_account_id, amount,
                   idempotency_key, requested_by_user_id)
                VALUES
                  (:id, CAST(:tipo AS transaction_type), :origem, :destino, :valor,
                   :chave, :user)
                """)
                .param("id", id)
                .param("tipo", tipo)
                .param("origem", origem)
                .param("destino", destino)
                .param("valor", new BigDecimal(valor))
                .param("chave", id.toString())
                .param("user", requester)
                .update();
        return id;
    }
}
