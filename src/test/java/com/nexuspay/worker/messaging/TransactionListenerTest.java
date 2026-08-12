package com.nexuspay.worker.messaging;

import com.nexuspay.worker.Fixtures;
import com.nexuspay.worker.PostgresTestBase;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A fila real e compartilhada com producao (secao 4 do spec). Nenhum teste
 * automatizado fala com ela — este roda inteiro contra LocalStack.
 */
class TransactionListenerTest extends PostgresTestBase {

    // withServices recebe String na Testcontainers 2.x — o enum Service so
    // existe na classe legada org.testcontainers.containers.localstack.
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3"))
                    .withServices("sqs");

    static SqsClient sqs;
    static String filaUrl;
    static String dlqUrl;

    static {
        LOCALSTACK.start();
    }

    @BeforeAll
    static void criarFilas() {
        sqs = SqsClient.builder()
                .endpointOverride(LOCALSTACK.getEndpoint())
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();

        dlqUrl = sqs.createQueue(CreateQueueRequest.builder()
                .queueName("teste-dlq.fifo")
                .attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true"))
                .build()).queueUrl();
        var dlqArn = sqs.getQueueAttributes(b -> b.queueUrl(dlqUrl)
                        .attributeNames(QueueAttributeName.QUEUE_ARN))
                .attributes().get(QueueAttributeName.QUEUE_ARN);

        filaUrl = sqs.createQueue(CreateQueueRequest.builder()
                .queueName("teste-principal.fifo")
                .attributes(Map.of(
                        QueueAttributeName.FIFO_QUEUE, "true",
                        QueueAttributeName.VISIBILITY_TIMEOUT, "1",
                        QueueAttributeName.REDRIVE_POLICY,
                        "{\"deadLetterTargetArn\":\"" + dlqArn + "\",\"maxReceiveCount\":2}"))
                .build()).queueUrl();
    }

    @DynamicPropertySource
    static void propriedadesDaFila(DynamicPropertyRegistry registry) {
        registry.add("nexuspay.sqs.queue-url", () -> filaUrl);
        registry.add("spring.cloud.aws.endpoint", () -> LOCALSTACK.getEndpoint().toString());
        // Religa o listener, que application-test.yml desliga para os demais testes.
        registry.add("nexuspay.sqs.listener-enabled", () -> "true");
    }

    @Autowired
    JdbcClient jdbc;

    private void publicar(UUID transactionId) {
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(filaUrl)
                .messageBody("{\"transaction_id\":\"" + transactionId + "\"}")
                .messageGroupId(UUID.randomUUID().toString())
                .messageDeduplicationId(transactionId.toString())
                .build());
    }

    private String status(UUID tx) {
        return jdbc.sql("SELECT status FROM transactions WHERE id = :id")
                .param("id", tx).query(String.class).single();
    }

    @Test
    void mensagem_publicada_move_o_saldo() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        publicar(tx);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(status(tx)).isEqualTo("COMPLETED"));
        assertThat(jdbc.sql("SELECT balance FROM accounts WHERE id = :id")
                .param("id", destino).query(BigDecimal.class).single())
                .isEqualByComparingTo(new BigDecimal("100.00"));
    }

    @Test
    void mensagem_com_uuid_desconhecido_e_consumida_sem_efeito() {
        var desconhecida = UUID.randomUUID();

        publicar(desconhecida);

        // Nada a assertar no banco: o efeito esperado e ausencia de efeito.
        // O que provamos e que a mensagem sai da fila em vez de voltar sempre.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var visiveis = sqs.getQueueAttributes(b -> b.queueUrl(filaUrl)
                            .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES))
                    .attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);
            assertThat(visiveis).isEqualTo("0");
        });
    }

    @Test
    void falha_de_negocio_termina_em_failed_e_a_mensagem_some() {
        var origem = Fixtures.criarConta(jdbc, "10.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        publicar(tx);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(status(tx)).isEqualTo("FAILED"));
    }

    @Test
    void mensagem_ilegivel_e_redirigida_para_a_dlq_apos_esgotar_as_tentativas() {
        // Corpo com transaction_id que nao e um UUID valido: falha na
        // conversao da mensagem, ANTES de chegar no TransactionProcessor.
        // E uma falha de infraestrutura genuina (mensagem envenenada), nao de
        // negocio — e por isso o listener nunca a confirma, a redrive policy
        // (maxReceiveCount=2, visibility timeout=1s) reentrega ate esgotar, e
        // a SQS move sozinha para a DLQ. Fecha a lacuna declarada no plano:
        // nenhum outro teste force esse caminho ate a DLQ.
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(filaUrl)
                .messageBody("{\"transaction_id\":\"nao-e-um-uuid\"}")
                .messageGroupId(UUID.randomUUID().toString())
                .messageDeduplicationId(UUID.randomUUID().toString())
                .build());

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var visiveisNaDlq = sqs.getQueueAttributes(b -> b.queueUrl(dlqUrl)
                            .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES))
                    .attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);
            assertThat(visiveisNaDlq).isEqualTo("1");
        });
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var visiveisNaPrincipal = sqs.getQueueAttributes(b -> b.queueUrl(filaUrl)
                            .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES))
                    .attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);
            assertThat(visiveisNaPrincipal).isEqualTo("0");
        });
    }
}
