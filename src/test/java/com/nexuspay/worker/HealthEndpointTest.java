package com.nexuspay.worker;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class HealthEndpointTest {

    @LocalServerPort
    int porta;

    // HttpClient do JDK de proposito: nenhum cliente HTTP do Spring, cujos
    // pacotes se reorganizaram na linha 4.0 do Boot. Uma dependencia a menos
    // para o teste mais simples da suite.
    @Test
    void expoe_health() throws Exception {
        var resposta = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + porta + "/actuator/health")).build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(resposta.statusCode()).isEqualTo(200);
        assertThat(resposta.body()).contains("UP");
    }
}
