package com.nexuspay.worker.config;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A resolucao de credencial, chamada direto — a classe e @Profile("!test")
 * de proposito, entao um SpringBootTest normal nunca a exercitaria.
 *
 * ACCESS KEY e o caminho de PRODUCAO (Render, Fly e equivalentes passam
 * segredo como variavel de ambiente comum). PERFIL continua sendo o caminho
 * de DESENVOLVIMENTO.
 */
class AwsConfigTest {

    private final AwsConfig config = new AwsConfig();

    @Test
    void com_access_key_usa_credencial_estatica() {
        AwsCredentialsProvider provider = config.awsCredentialsProvider(
                "", "AKIAEXEMPLO", "segredo-de-teste");

        assertThat(provider).isInstanceOf(StaticCredentialsProvider.class);
        assertThat(provider.resolveCredentials().accessKeyId()).isEqualTo("AKIAEXEMPLO");
        assertThat(provider.resolveCredentials().secretAccessKey()).isEqualTo("segredo-de-teste");
    }

    @Test
    void sem_access_key_cai_no_perfil() {
        // O caminho de desenvolvimento nao pode quebrar: quem ja tem
        // ~/.aws/credentials configurado localmente continua funcionando.
        AwsCredentialsProvider provider = config.awsCredentialsProvider(
                "nexuspay-worker", "", "");

        assertThat(provider).isInstanceOf(ProfileCredentialsProvider.class);
    }

    @Test
    void nem_perfil_nem_access_key_e_erro_proprio_nao_silencio() {
        // A cadeia padrao do SDK procuraria variaveis de ambiente e
        // metadados de instancia antes de desistir — exatamente o que ja
        // custou caro na 2a. Falhar aqui, com mensagem propria, e o
        // comportamento certo.
        assertThatThrownBy(() -> config.awsCredentialsProvider("", "", ""))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("nexuspay.aws.profile");
    }

    @Test
    void access_key_sem_secret_cai_no_perfil_em_vez_de_credencial_incompleta() {
        // As duas metades do par precisam estar presentes; uma so nao basta
        // para montar uma credencial valida.
        AwsCredentialsProvider provider = config.awsCredentialsProvider(
                "nexuspay-worker", "AKIAEXEMPLO", "");

        assertThat(provider).isInstanceOf(ProfileCredentialsProvider.class);
    }
}
