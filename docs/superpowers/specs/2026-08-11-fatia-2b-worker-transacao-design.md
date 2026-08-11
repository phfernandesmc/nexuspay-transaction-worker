# Fatia 2b — Worker de transação

Consumidor da fila SQS FIFO que executa o movimento de saldo com garantias ACID e registra a auditoria. Sem API de negócio.

Repositório: `nexuspay-transaction-worker`.

---

## 1. O que a Fatia 2a deixa pronto

- A tabela `transactions` existe com todas as constraints. O worker lê a linha e atualiza `status`, `failure_reason` e `updated_at`.
- `accounts.balance` não tem nenhum outro escritor. O gateway nunca escreve nessa coluna; o único caminho fora do worker é `scripts/seed_dev.py`, que se recusa a rodar em produção.
- `accounts` tem `CHECK (balance >= 0)`. É a última rede do sistema contra saldo negativo, abaixo de qualquer lógica de aplicação.
- `MessageGroupId` é a conta de **origem** (destino, no depósito), então duas saídas da mesma conta nunca chegam em paralelo.
- A varredura do gateway republica transação `PENDING` presa há mais de 10 minutos, então o worker pode morrer no meio sem perder transação.
- A mensagem carrega apenas `{"transaction_id": "<uuid>"}`. `MessageDeduplicationId` é o id da transação.

## 2. Escopo

**Dentro:** consumo da fila, execução do movimento de saldo, lançamentos de auditoria, status final, classificação de erro, DLQ, `/actuator/health`.

**Fora:** deploy (Fatia 4), frontend (Fatia 3), qualquer endpoint de negócio, estorno/reversão de transação concluída, e leitura da própria DLQ pela aplicação.

## 3. Stack e versões

| | Escolha | Por quê |
|---|---|---|
| Java | 25 (Temurin 25.0.4 LTS) | Já instalado; decidido na 2a |
| Spring Boot | **4.0.x** | A linha 3.5 teve sua última release OSS em 25/06/2026. Num serviço que move dinheiro, ficar sem patch de segurança aberto não se justifica. A documentação oficial de 4.0 declara compatibilidade "up to and including Java 26" |
| Spring Cloud AWS | **4.x** | A matriz oficial pareia 4.x com Boot 4.0.x e Spring Framework 7.0.x. A 3.4.x é a última compatível com Boot 3.5.x |
| Acesso a dados | **`JdbcClient`**, não JPA | O worker faz meia dúzia de instruções cuja semântica de trava é o produto. Um mapeador que gera SQL implícito e escreve por *dirty checking* esconde justamente o que precisa estar visível na revisão |
| Build | **Maven 3.9.16** com wrapper versionado | Maven 4 ainda está em `4.0.0-rc-6`. O wrapper é obrigatório de qualquer forma: a máquina não tinha Maven no PATH |
| Testes | Testcontainers (PostgreSQL 16 + LocalStack) | §12 |

O Maven foi instalado em `%LOCALAPPDATA%\Programs\apache-maven-3.9.16`, com SHA-512 conferido contra o publicado pela Apache.

## 4. Infraestrutura AWS — estado verificado

Medido com as credenciais reais, não presumido.

| Item | Valor |
|---|---|
| Fila principal | `arn:aws:sqs:us-east-1:797771596673:api-processar-transferencia-worker.fifo` |
| DLQ | `arn:aws:sqs:us-east-1:797771596673:api-processar-transferencia-worker-dlq.fifo` |
| Redrive | ativa, `maxReceiveCount = 10` |
| Visibility timeout da principal | 120 s |
| Criptografia | SSE-SQS gerenciada (`SqsManagedSseEnabled = true`), **sem** chave KMS própria |
| FIFO de alto throughput | desligada; escopo de dedup por fila |

**Policy do usuário `backend-java-worker`**, sobre o ARN da fila principal — as cinco ações verificadas como permitidas:

```
sqs:ReceiveMessage
sqs:DeleteMessage
sqs:GetQueueAttributes
sqs:GetQueueUrl
sqs:ChangeMessageVisibility
```

Três consequências dessa configuração, todas verificadas empiricamente:

1. **Nenhuma permissão de KMS é necessária.** Com SSE-SQS gerenciada não há chave do cliente a decifrar. Se a fila migrar para uma chave própria, o worker passa a precisar de `kms:Decrypt` e `kms:GenerateDataKey`, e o gateway também.
2. **Nenhuma ação de lote é necessária.** `DeleteMessageBatch` e `ChangeMessageVisibilityBatch` são autorizadas pelas permissões singulares. Isso importa porque o Spring Cloud AWS confirma mensagens em lote por padrão — se fossem ações separadas, o worker quebraria só sob carga.
3. **Nada é concedido sobre a DLQ, de propósito.** O redrive é executado pela própria SQS, sem principal. A aplicação não deve conseguir consumir a própria DLQ, senão a mensagem envenenada volta a circular. Inspeção da DLQ é trabalho de console.

A separação de privilégio foi confirmada nos dois sentidos: `backend-fastapi-producer` não consegue ler a fila, `backend-java-worker` não consegue escrever nela.

**Dívida conhecida, fora do escopo desta fatia:** `sqs:CreateQueue` está permitido nos dois usuários, o que contraria a separação pretendida.

## 5. Schema — `ledger_entries`

A migration é escrita **no repositório do gateway**. O Alembic segue dono único do schema; o worker roda zero migrations e não tem Flyway nem Liquibase.

| coluna | tipo | restrição |
|---|---|---|
| `id` | UUID | PK |
| `transaction_id` | UUID | FK → `transactions(id)` `ON DELETE RESTRICT` |
| `account_id` | UUID | FK → `accounts(id)` `ON DELETE RESTRICT` |
| `direction` | enum `ledger_direction` | `DEBIT` \| `CREDIT` |
| `amount` | NUMERIC(15,2) | `CHECK (amount > 0)` |
| `balance_after` | NUMERIC(15,2) | `CHECK (balance_after >= 0)` |
| `created_at` | TIMESTAMPTZ | `NOT NULL DEFAULT now()` |

- `UNIQUE (transaction_id, account_id)` — uma transação toca cada conta no máximo uma vez. É a última rede contra aplicação dupla.
- `INDEX (account_id, created_at DESC, id DESC)` — para consulta de histórico por conta.

Uma transferência gera dois lançamentos; um depósito gera um. Não há partida dobrada com conta de contrapartida: o depósito traz dinheiro de fora do sistema, e inventar uma conta de clearing só para equilibrar linhas acrescentaria um conceito que ninguém consulta.

`balance_after` é o que transforma auditoria em algo verificável: o saldo passa a ser reconstruível a partir do histórico, e divergência entre `accounts.balance` e o último `balance_after` vira detectável.

## 6. Ciclo de vida de uma mensagem

Tudo dentro de uma única transação de banco.

1. `SELECT ... FROM transactions WHERE id = :id FOR UPDATE`
   Não encontrada → no-op **terminal**: loga e deleta a mensagem. É o caso de mensagem gerada por outro ambiente contra a fila compartilhada (2a §10.2). Insistir faria a mensagem voltar para sempre.
2. `status != PENDING` → no-op: deleta a mensagem. É a redelivery de algo já resolvido.
3. `SAVEPOINT`, e dentro dele o movimento de saldo (§7).
4. `INSERT` dos lançamentos, ainda dentro do savepoint.
5. Em falha de negócio, `ROLLBACK TO SAVEPOINT` — o movimento some, a trava do passo 1 permanece.
6. `UPDATE transactions SET status = ..., failure_reason = ..., updated_at = now()`, fora do savepoint.
7. Commit → confirma a mensagem.

O passo 1 é o que fecha a redelivery de verdade. Dois consumidores com a mesma mensagem: o segundo **bloqueia** no `FOR UPDATE` até o primeiro commitar, então lê `COMPLETED` e desiste no passo 2. A única do ledger é a rede de baixo, não a de cima.

## 7. Movimento de saldo

Instrução condicional única: a checagem e a escrita acontecem juntas, sem intervalo entre elas. Isso **elimina** o check-then-act em vez de protegê-lo com trava — o formato de defeito que apareceu quatro vezes no gateway.

```sql
-- débito (só transferência)
UPDATE accounts SET balance = balance - :amount, updated_at = now()
 WHERE id = :source AND status = 'ACTIVE' AND balance >= :amount
RETURNING balance;

-- crédito
UPDATE accounts SET balance = balance + :amount, updated_at = now()
 WHERE id = :destination AND status = 'ACTIVE'
RETURNING balance;
```

- **Zero linhas afetadas já é a resposta de negócio.** Não há leitura anterior em que confiar.
- `RETURNING` entrega o `balance_after` do lançamento sem um `SELECT` extra.
- Numa transferência, os dois `UPDATE` saem **em ordem crescente do UUID da conta**. `MessageGroupId` é a conta de origem, então A→B e B→A caem em grupos diferentes e podem ser processadas em paralelo, tocando as mesmas duas linhas em ordem oposta. Sem a ordenação, isso é deadlock.
- O `CHECK (balance >= 0)` da tabela permanece como rede final.

### 7.1 SAVEPOINT — por que ele é obrigatório aqui

A ordenação por UUID significa que o **crédito pode sair antes do débito**. Se o débito então devolver zero linhas por saldo insuficiente, o crédito já aplicado precisa desaparecer — mas o `status = FAILED` precisa **sobreviver**. As duas coisas estão na mesma transação, e um rollback simples desfaria as duas, deixando a transação em `PENDING` para a varredura republicar em ciclo infinito.

Por isso o movimento de saldo e os lançamentos ficam dentro de um **SAVEPOINT**:

```
BEGIN
  SELECT ... FROM transactions WHERE id = :id FOR UPDATE   -- fora do savepoint
  SAVEPOINT movimento
    UPDATE accounts ... (em ordem de UUID)
    INSERT INTO ledger_entries ...
  -- falha de negócio: ROLLBACK TO SAVEPOINT movimento
  UPDATE transactions SET status = COMPLETED | FAILED, ...
COMMIT
```

A trava do passo 1 fica **fora** do savepoint, então ela sobrevive ao rollback parcial e continua serializando consumidores concorrentes até o commit. É a mesma técnica que `AccountRepository.create` já usa no gateway para colisão de número de conta.

Isso fecha os dois requisitos que a 2a registrou em `follow-ups-fatia-2a.md`: a transferência concorrente que o gateway aceita indevidamente termina em `FAILED` aqui, e creditar conta encerrada deixa de ser possível.

## 8. Classificação de erro

Só duas categorias importam, porque decidem se a mensagem morre ou volta.

| Situação | Transação | Mensagem |
|---|---|---|
| UUID não existe no banco | nada | deleta (no-op terminal) |
| `status` já não é `PENDING` | nada | deleta |
| débito com 0 linhas, conta ativa e saldo menor que o valor | `FAILED` / `INSUFFICIENT_FUNDS` | deleta |
| origem inexistente ou não `ACTIVE` | `FAILED` / `SOURCE_ACCOUNT_UNAVAILABLE` | deleta |
| destino inexistente ou não `ACTIVE` | `FAILED` / `DESTINATION_ACCOUNT_UNAVAILABLE` | deleta |
| violação da única do ledger | rollback | deleta — outro consumidor já aplicou |
| **qualquer exceção de infraestrutura** | rollback | **não deleta** → reentrega → 10ª vez vai para a DLQ |

Distinguir "saldo insuficiente" de "conta indisponível" exige um `SELECT` de diagnóstico depois das zero linhas — apenas para preencher `failure_reason`, **nunca** para decidir. A decisão já foi tomada pelo `UPDATE`.

`failure_reason` é `VARCHAR(255)` e recebe um conjunto fechado de códigos, não texto livre: o frontend da Fatia 3 vai traduzir por código.

## 9. DLQ e o ciclo com a varredura

Com `maxReceiveCount = 10` e visibility timeout de 120 s, uma mensagem envenenada segura o `MessageGroupId` por cerca de 18 a 20 minutos antes de sair para a DLQ. Como o grupo é a conta de origem, isso significa que as saídas daquela conta ficam paradas nesse intervalo.

**Consequência declarada, não escondida:** quando a mensagem vai para a DLQ, a transação continua `PENDING`. A varredura do gateway republica transação `PENDING` presa há mais de 10 minutos, então ela volta, falha 10 vezes de novo, e vai para a DLQ de novo — em ciclo.

Isso é o comportamento **correto** para falha de infraestrutura, que se cura sozinha assim que o banco volta. Para bug genuíno é ruído crescente até alguém corrigir, na ordem de uma centena de mensagens por dia para uma única transação quebrada. **O sinal de alerta é a profundidade da DLQ**, não a da fila principal. A retenção de 14 dias na DLQ existe para esse rastro sobreviver a um fim de semana.

## 10. Configuração

Nenhum segredo em arquivo versionado. As credenciais AWS vêm de perfil nomeado em `~/.aws/credentials`, igual ao gateway.

| Chave | Valor local | Observação |
|---|---|---|
| perfil AWS | `nexuspay-worker` | nunca o do produtor |
| região | `us-east-1` | explícita, não herdada do ambiente |
| URL da fila | a da fila principal | |
| URL do banco | Postgres em `localhost:5433` | o 5432 costuma estar ocupado por serviço nativo no Windows |

A porta 5433 e a exigência de região explícita são lições da 2a: o gateway quebrou em produção porque o `pydantic-settings` carrega o `.env` para o objeto de configuração, não para o ambiente do processo, e o SDK não enxergava a região. O equivalente aqui é não depender de `AWS_REGION` do ambiente.

## 11. Health e observabilidade

O worker não tem API de negócio. A única porta HTTP é `/actuator/health`, que existe porque o gateway depende dela para o truque anti-scale-to-zero (2a §8.4). Não expõe dado de negócio.

Log estruturado por mensagem, com o id da transação em todas as linhas: recebida, decisão tomada, resultado. Os no-ops (UUID inexistente, status já resolvido) são logados explicitamente — são eles que explicam uma fila que consome sem nada mudar no banco.

## 12. Testes

Testcontainers com **PostgreSQL 16** (a mesma imagem do `docker-compose.yml` do gateway) e **LocalStack** para SQS. Isolamento total: nenhum teste consegue tocar a fila real, nem por acidente. Do lado do consumidor isso é mais crítico do que era na 2a — uma suíte apontada para a fila compartilhada come transferências reais por construção.

Mais **um** teste marcado, pulado automaticamente sem credenciais, que fala com a fila de verdade só para provar IAM e semântica FIFO — mesmo padrão do `requires_sqs` da 2a.

O que precisa de cobertura, e que só existe com SQS de verdade (ainda que emulado): redelivery por visibility timeout, ordenação por `MessageGroupId`, e o caminho até a DLQ.

**O ponto fraco declarado.** O schema mora no outro repositório, e os testes do worker precisam dele. Nenhuma opção é limpa: versionar um dump SQL cria risco de divergência silenciosa quando o gateway migrar; rodar o Alembic a partir do Java é atravessar linguagem. A decisão é versionar o dump gerado por um script documentado **e** manter um teste que falha se as colunas de que o worker depende não existirem, para a divergência aparecer como falha de teste em vez de bug em produção. É a parte menos satisfatória deste desenho.

## 13. Riscos aceitos

1. **Ciclo DLQ ↔ varredura** para bug genuíno (§9). Mitigação: profundidade da DLQ como alarme.
2. **Divergência de schema entre repositórios** (§12). Mitigação: teste de colunas.
3. **Fila compartilhada entre desenvolvimento e produção.** Decisão do dono do projeto, mantida. Mitigação: a mensagem carrega só o UUID, e cada ambiente tem seu banco — um worker que não acha o UUID não move saldo nenhum (2a §10.2).
4. **`sqs:CreateQueue` concedido a mais** nos dois usuários. Fora do escopo desta fatia.

## 14. Critérios de aceitação

1. Transferência válida sai de `PENDING` para `COMPLETED`, com saldo de origem e destino corretos e dois lançamentos gravados.
2. Depósito válido gera um lançamento e credita o destino.
3. Transferência sem saldo termina `FAILED` / `INSUFFICIENT_FUNDS`, com saldo intacto e nenhum lançamento.
4. Transferência para conta encerrada termina `FAILED` / `DESTINATION_ACCOUNT_UNAVAILABLE`.
5. A mesma mensagem entregue duas vezes aplica o movimento **uma** vez.
6. Duas transferências concorrentes de R$ 100 contra saldo de R$ 100: uma `COMPLETED`, outra `FAILED`, saldo final zero, nunca negativo.
7. A→B e B→A concorrentes concluem as duas, sem deadlock.
7a. Transferência em que o UUID do destino é **menor** que o da origem — logo o crédito sai primeiro — e o saldo é insuficiente: a conta de destino termina com o saldo **inalterado** e a transação em `FAILED`. É o teste que prova o SAVEPOINT da §7.1; sem ele o destino ficaria creditado numa transferência que falhou.
8. UUID inexistente no banco: mensagem deletada, nada gravado, log explícito.
9. Falha de infraestrutura não deleta a mensagem, e ela reaparece.
10. Depois de 10 recebimentos a mensagem está na DLQ e o `MessageGroupId` volta a fluir.
11. `accounts.balance` nunca fica negativo em nenhum cenário de teste.

## 15. Ponte para as próximas fatias

- A Fatia 3 (frontend) ganha `failure_reason` como conjunto fechado de códigos para traduzir, e `ledger_entries` como fonte de um extrato com saldo.
- A Fatia 4 (deploy) precisa resolver: `WORKER_HEALTH_URL` apontando para o worker de verdade, e alarme de CloudWatch sobre a profundidade da DLQ.
- O que esta fatia **não** resolve e alguém terá de decidir: estorno de transação já concluída, e reprocessamento manual do que caiu na DLQ.
