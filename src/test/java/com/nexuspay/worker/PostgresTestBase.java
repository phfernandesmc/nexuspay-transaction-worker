package com.nexuspay.worker;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * PostgreSQL 16 descartavel, com o schema do gateway aplicado.
 *
 * A imagem e a mesma do docker-compose.yml do gateway: rodar teste contra uma
 * versao diferente da de producao esconde diferenca de comportamento em
 * exatamente as areas que importam aqui — trava de linha e RETURNING.
 *
 * O container e estatico e iniciado uma vez para toda a suite; o Testcontainers
 * o derruba no fim da JVM. Cada teste limpa o que criou.
 */
@SpringBootTest
@ActiveProfiles("test")
public abstract class PostgresTestBase {

    // Sem parametro generico: na Testcontainers 2.x,
    // org.testcontainers.postgresql.PostgreSQLContainer NAO e uma classe
    // generica — escrever PostgreSQLContainer<?> nao compila. A classe
    // generica antiga (org.testcontainers.containers.PostgreSQLContainer)
    // ainda existe no jar, mas e a forma legada.
    static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:16"))
                    .withDatabaseName("nexuspay")
                    .withUsername("nexuspay")
                    .withPassword("nexuspay")
                    .withInitScript("schema.sql");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void propriedades(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
}
