package com.nexuspay.worker;

import java.net.URI;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Prova que o IAM e a semantica FIFO estao como o spec descreve.
 *
 * Desligado por padrao. Rode com:  ./mvnw test -Dfila.real=true
 *
 * NAO publica nem consome nada: a fila e compartilhada com producao, e na
 * fatia 2a um teste que consumia dela chegou a deixar transferencias reais
 * invisiveis. Aqui so lemos atributos.
 */
@EnabledIfSystemProperty(named = "fila.real", matches = "true")
class RealQueueSmokeTest {

    private static final String FILA =
            "https://sqs.us-east-1.amazonaws.com/797771596673/api-processar-transferencia-worker.fifo";

    private SqsClient cliente() {
        return SqsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create("nexuspay-worker"))
                .endpointOverride(URI.create("https://sqs.us-east-1.amazonaws.com"))
                .build();
    }

    @Test
    void a_fila_e_fifo_e_tem_redrive_para_a_dlq() {
        var atributos = cliente().getQueueAttributes(b -> b.queueUrl(FILA)
                .attributeNames(QueueAttributeName.FIFO_QUEUE,
                        QueueAttributeName.REDRIVE_POLICY)).attributes();

        assertThat(atributos.get(QueueAttributeName.FIFO_QUEUE)).isEqualTo("true");
        assertThat(atributos.get(QueueAttributeName.REDRIVE_POLICY))
                .contains("api-processar-transferencia-worker-dlq.fifo")
                .contains("maxReceiveCount");
    }

    @Test
    void o_perfil_do_worker_resolve_a_fila_por_nome() {
        // O @SqsListener resolve fila por nome, o que e um GetQueueUrl. Sem a
        // permissao, a SQS devolve NonExistentQueue em vez de AccessDenied e o
        // worker quebraria so no boot.
        var url = cliente().getQueueUrl(b -> b.queueName(
                "api-processar-transferencia-worker.fifo")).queueUrl();

        assertThat(url).isEqualTo(FILA);
    }
}
