package com.nexuspay.worker.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * TransactionTemplate com PROPAGATION_NESTED, que sobre JDBC vira SAVEPOINT.
 *
 * Deliberadamente programatico, e nao @Transactional(propagation = NESTED) num
 * metodo da mesma classe: anotacao so vale quando a chamada passa pelo proxy do
 * Spring, e uma chamada de um metodo para outro do MESMO objeto nao passa. Esse
 * detalhe silencioso transformaria o savepoint em nada, e o teste que o cobre
 * passaria por acidente enquanto producao ficaria errada.
 */
@Configuration
class NestedTransactionConfig {

    @Bean
    TransactionTemplate nestedTransactionTemplate(PlatformTransactionManager manager) {
        var template = new TransactionTemplate(manager);
        template.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
        return template;
    }
}
