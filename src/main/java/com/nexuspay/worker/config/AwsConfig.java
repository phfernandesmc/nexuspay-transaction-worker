package com.nexuspay.worker.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Credencial explicita, por um dos dois caminhos abaixo. Fora do perfil de teste.
 *
 * ACCESS KEY primeiro: e o caminho de PRODUCAO. Render, Fly e toda PaaS
 * equivalente passam segredo como variavel de ambiente comum, sem a
 * convencao de "perfil nomeado" de ~/.aws/credentials — essa convencao so
 * existe na maquina de quem desenvolve.
 *
 * PERFIL como fallback: e o caminho de DESENVOLVIMENTO, sem exigir nenhuma
 * mudanca de quem ja tem ~/.aws/credentials configurado.
 *
 * Nenhum dos dois configurado e erro proprio, e nao silencio: a cadeia
 * padrao do SDK procuraria variaveis de ambiente e metadados de instancia
 * antes de desistir, e na 2a isso ja custou caro no gateway — a configuracao
 * carregada para dentro do objeto de settings nao chega ao ambiente do
 * processo, e o SDK acaba resolvendo algo diferente do que o arquivo de
 * configuracao diz.
 *
 * O gatilho e @Profile("!test"), nao @ConditionalOnProperty. A condicao
 * anterior casava por EXISTENCIA da chave nexuspay.aws.profile, e
 * application.yml sempre a define — entao o javadoc que dizia "os testes, que
 * nao definem nexuspay.aws.profile, caem nas credenciais estaticas do
 * LocalStack" descrevia algo que nunca aconteceu. O bean era criado em TODO
 * contexto de teste, e como CredentialsProviderAutoConfiguration so vale na
 * ausencia de um bean AwsCredentialsProvider (ConditionalOnMissingBean), a
 * presenca dele desligava a autoconfiguracao inteira: o bloco
 * spring.cloud.aws.credentials de application-test.yml nunca tinha efeito e
 * todo cliente AWS da suite era assinado com a credencial REAL de producao.
 *
 * Com @Profile("!test") a afirmacao passa a ser verdadeira: no perfil de teste
 * esta classe nao e registrada, a autoconfiguracao volta a valer e as
 * credenciais estaticas de application-test.yml sao as que assinam os clientes
 * contra o LocalStack.
 */
@Configuration
@Profile("!test")
class AwsConfig {

    @Bean
    AwsCredentialsProvider awsCredentialsProvider(
            @Value("${nexuspay.aws.profile:}") String perfil,
            @Value("${nexuspay.aws.access-key-id:}") String accessKeyId,
            @Value("${nexuspay.aws.secret-access-key:}") String secretAccessKey) {
        if (!accessKeyId.isBlank() && !secretAccessKey.isBlank()) {
            return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey));
        }
        if (!perfil.isBlank()) {
            return ProfileCredentialsProvider.create(perfil);
        }
        throw new IllegalStateException(
                "Configure nexuspay.aws.profile (desenvolvimento) ou "
                        + "nexuspay.aws.access-key-id + nexuspay.aws.secret-access-key "
                        + "(producao) — nenhuma credencial e assumida implicitamente.");
    }
}
