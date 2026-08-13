package com.nexuspay.worker.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

/**
 * Credencial por perfil nomeado, explicita. Fora do perfil de teste.
 *
 * A cadeia padrao do SDK procuraria variaveis de ambiente e metadados de
 * instancia antes do perfil, e na 2a isso ja custou caro no gateway: a
 * configuracao carregada para dentro do objeto de settings nao chega ao
 * ambiente do processo, e o SDK acaba resolvendo algo diferente do que o
 * arquivo de configuracao diz.
 *
 * O gatilho e @Profile("!test"), nao mais @ConditionalOnProperty. A condicao
 * anterior casava por EXISTENCIA da chave nexuspay.aws.profile, e
 * application.yml sempre a define — entao o javadoc que dizia "os testes, que
 * nao definem nexuspay.aws.profile, caem nas credenciais estaticas do
 * LocalStack" descrevia algo que nunca aconteceu. O bean era criado em TODO
 * contexto de teste, e como CredentialsProviderAutoConfiguration so vale na
 * ausencia de um bean AwsCredentialsProvider (ConditionalOnMissingBean), a
 * presenca dele desligava a autoconfiguracao inteira: o bloco
 * spring.cloud.aws.credentials de
 * application-test.yml nunca tinha efeito e todo cliente AWS da suite era
 * assinado com a credencial REAL de producao.
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
    AwsCredentialsProvider awsCredentialsProvider(@Value("${nexuspay.aws.profile}") String perfil) {
        return ProfileCredentialsProvider.create(perfil);
    }
}
