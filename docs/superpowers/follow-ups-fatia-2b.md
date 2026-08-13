# Follow-ups da Fatia 2b

## Precisa sair antes do deploy (Fatia 4)

### Alarme sobre a profundidade da DLQ

Quando uma mensagem vai para a DLQ, a transação continua `PENDING` e a
varredura do gateway a republica, em ciclo. Isso é correto para falha de
infraestrutura, que se cura sozinha; para bug genuíno é ruído crescente. **A
profundidade da DLQ é o sinal de alerta**, e hoje ninguém a observa. Precisa de
um alarme de CloudWatch.

### `WORKER_HEALTH_URL` do gateway continua vazia

O gateway já tem o disparo fire-and-forget contra o `/health` do worker, mas a
variável está vazia em desenvolvimento. Só faz sentido preencher quando existir
um worker publicado.

## Dívida de infraestrutura

## Fora de escopo, mas alguém vai perguntar

- **Estorno de transação já concluída.** Não existe caminho. Provavelmente é uma
  transação nova em sentido inverso, com referência à original, e não um
  `UPDATE` na linha antiga.
- **Reprocessamento manual do que caiu na DLQ.** Hoje é trabalho de console.

## Dívida de cobertura de teste

### A asserção "fila principal volta a 0" some do teste da DLQ

`mensagem_ilegivel_vai_para_a_dlq_e_libera_o_grupo_para_a_proxima_mensagem`
(`TransactionListenerTest`) tinha, num round de correção anterior, uma
asserção explícita de que `APPROXIMATE_NUMBER_OF_MESSAGES` da fila principal
voltava a `0` depois que a mensagem envenenada saía dela. Essa asserção foi
removida durante uma rodada de correção e nunca recolocada. Hoje o teste
infere o mesmo fato indiretamente: ele afirma que há 1 mensagem visível na
DLQ e que a segunda mensagem (mesmo `MessageGroupId`) chega a `COMPLETED`,
o que só é possível se a primeira já não estiver mais bloqueando o grupo na
fila principal. É uma inferência válida, mas não é a mesma cobertura direta
que existia antes — vale reavaliar se recolocar a asserção explícita compensa
a fragilidade adicional de depender de contagem de mensagens visíveis (que
pode oscilar por causa de timing do LocalStack).

### `nestedTransactionTemplate` é o único `TransactionTemplate` do contexto

`NestedTransactionConfig` declara um único bean `TransactionTemplate`, com
`PROPAGATION_NESTED` para virar `SAVEPOINT` sobre JDBC (ver §7.1 do spec).
Como é o único bean desse tipo no contexto Spring, qualquer código futuro que
injete `TransactionTemplate` por tipo — sem saber que precisa pedir
`REQUIRED` explicitamente, ou sem saber que este bean existe — recebe esse
mesmo `nestedTransactionTemplate` por injeção automática, com propagação
`NESTED` em vez do `REQUIRED` que normalmente se espera de um
`TransactionTemplate` sem qualificador. Isso é silencioso: não há erro de
compilação nem de boot, só um comportamento transacional diferente do
esperado. Se um segundo caso de uso precisar de uma transação `REQUIRED`
"normal", declarar um bean nomeado e explícito para não colidir com este.
