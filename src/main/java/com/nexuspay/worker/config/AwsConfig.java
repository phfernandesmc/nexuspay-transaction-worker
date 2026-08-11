package com.nexuspay.worker.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

/**
 * Credencial por perfil nomeado, explicita.
 *
 * A cadeia padrao do SDK procuraria variaveis de ambiente e metadados de
 * instancia antes do perfil, e na 2a isso ja custou caro no gateway: a
 * configuracao carregada para dentro do objeto de settings nao chega ao
 * ambiente do processo, e o SDK acaba resolvendo algo diferente do que o
 * arquivo de configuracao diz.
 *
 * Condicional na propriedade para que os testes, que nao definem
 * nexuspay.aws.profile, caiam nas credenciais estaticas do LocalStack sem
 * precisar sobrescrever bean.
 */
@Configuration
@ConditionalOnProperty(name = "nexuspay.aws.profile")
class AwsConfig {

    @Bean
    AwsCredentialsProvider awsCredentialsProvider(@Value("${nexuspay.aws.profile}") String perfil) {
        return ProfileCredentialsProvider.create(perfil);
    }
}
