# Fatia 2b — Worker de Transação: Plano de Implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir o consumidor da fila SQS FIFO que executa o movimento de saldo com garantias ACID e grava a auditoria em `ledger_entries`.

**Architecture:** Um `@SqsListener` recebe `{"transaction_id": "<uuid>"}` e delega a um processador transacional. O processador trava a linha da transação com `SELECT ... FOR UPDATE` (que é o que fecha a redelivery), aplica o saldo por `UPDATE` condicional com `RETURNING` dentro de um SAVEPOINT (que é o que elimina o check-then-act e permite desfazer o movimento mantendo o `FAILED`), grava os lançamentos e escreve o status final.

**Tech Stack:** Java 25, Spring Boot 4.0.7, Spring Cloud AWS 4.1.0, `JdbcClient` (sem JPA), Maven 3.9.16 com wrapper, Testcontainers 2.0.5 (PostgreSQL 16 + LocalStack).

**Spec:** `docs/superpowers/specs/2026-08-11-fatia-2b-worker-transacao-design.md`

## Global Constraints

Todas as tarefas herdam estas restrições.

- **Versões fixas, verificadas no Maven Central:** Spring Boot `4.0.7`, Spring Cloud AWS `4.1.0`, Testcontainers `2.0.5`, `java.version` = `25`. Não subir para Boot 4.1.x: o Spring Cloud AWS 4.1.0 é construído sobre `spring-cloud-build` 5.0.2, cuja propriedade `spring-boot.version` é `4.0.7`.
- **Coordenadas do Testcontainers 2.x têm prefixo:** `org.testcontainers:testcontainers-postgresql`, `testcontainers-localstack`, `testcontainers-junit-jupiter`. As coordenadas antigas (`org.testcontainers:postgresql`) **não existem** na 2.x.
- **O worker roda ZERO migrations.** Nada de Flyway ou Liquibase. O Alembic no repositório `nexuspay-api-gateway` é dono único do schema.
- **`accounts.balance` só é escrito por este worker.** Nenhum outro caminho.
- **Enum do PostgreSQL exige cast explícito na escrita.** `UPDATE transactions SET status = :status` falha com *"column status is of type transaction_status but expression is of type character varying"*, porque o driver JDBC envia `varchar`. Use sempre `CAST(:status AS transaction_status)` e `CAST(:direction AS ledger_direction)`. Na leitura, `rs.getString(...)` funciona direto. Em cláusula `WHERE` com valor fixo, use literal (`status = 'ACTIVE'`), que não precisa de cast.
- **Ordem dos `UPDATE` em transferência:** sempre crescente pelo UUID da conta. `MessageGroupId` é a conta de origem, então A→B e B→A rodam em paralelo e tocam as mesmas linhas em ordem oposta.
- **Perfil AWS:** `nexuspay-worker`, região `us-east-1`, ambos explícitos. Nunca herdados do ambiente.
- **Fila principal:** `https://sqs.us-east-1.amazonaws.com/797771596673/api-processar-transferencia-worker.fifo` — **compartilhada com produção**. Nenhum teste automatizado, exceto o da Task 10, pode falar com ela.
- **Pacote raiz:** `com.nexuspay.worker`.
- **Commits em português**, no formato `tipo: descrição` já usado nos outros repositórios, terminando com `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.

## Estrutura de arquivos

**Repositório `nexuspay-transaction-worker`:**

```
pom.xml                                    dependências e versões fixadas
mvnw, mvnw.cmd, .mvn/wrapper/              wrapper versionado
scripts/regenerate-schema.sh               regenera o dump do schema a partir do gateway
src/main/java/com/nexuspay/worker/
  WorkerApplication.java                   ponto de entrada
  config/AwsConfig.java                    credencial por perfil, explícita
  config/NestedTransactionConfig.java      TransactionTemplate com PROPAGATION_NESTED
  domain/TransactionType.java              DEPOSIT | TRANSFER
  domain/TransactionStatus.java            PENDING | COMPLETED | FAILED
  domain/LedgerDirection.java              DEBIT | CREDIT
  domain/FailureReason.java                conjunto fechado de códigos
  domain/TransactionRecord.java            linha lida de transactions
  domain/BusinessFailure.java              exceção interna carregando FailureReason
  persistence/TransactionRepository.java   findForUpdate, markCompleted, markFailed
  persistence/AccountRepository.java       debit, credit, findStatus
  persistence/LedgerRepository.java        insert
  service/TransactionProcessor.java        o ciclo de vida completo
  messaging/TransactionMessage.java        record do corpo da mensagem
  messaging/TransactionListener.java       @SqsListener
src/main/resources/application.yml
src/test/resources/schema.sql              dump gerado pelo script
src/test/java/com/nexuspay/worker/         testes
```

**Repositório `nexuspay-api-gateway` (só a Task 2):**

```
app/domains/ledger/__init__.py
app/domains/ledger/models.py               modelo SQLAlchemy de ledger_entries
alembic/env.py                             +1 import
alembic/versions/<rev>_ledger_entries.py   migration
tests/integration/test_ledger_schema.py    teste do round-trip
```

---

### Task 1: Esqueleto do projeto que sobe e responde `/actuator/health`

**Files:**
- Create: `pom.xml`
- Create: `src/main/java/com/nexuspay/worker/WorkerApplication.java`
- Create: `src/main/java/com/nexuspay/worker/config/AwsConfig.java`
- Create: `src/main/resources/application.yml`
- Create: `src/test/resources/application-test.yml`
- Test: `src/test/java/com/nexuspay/worker/HealthEndpointTest.java`
- Create (gerado): `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties`

**Interfaces:**
- Consumes: nada.
- Produces: aplicação Spring Boot no pacote `com.nexuspay.worker`; propriedades `nexuspay.aws.profile`, `nexuspay.aws.region`, `nexuspay.sqs.queue-url`.

- [ ] **Step 1: Criar o `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>4.0.7</version>
    <relativePath/>
  </parent>

  <groupId>com.nexuspay</groupId>
  <artifactId>transaction-worker</artifactId>
  <version>0.0.1-SNAPSHOT</version>
  <name>nexuspay-transaction-worker</name>

  <properties>
    <java.version>25</java.version>
    <spring-cloud-aws.version>4.1.0</spring-cloud-aws.version>
    <testcontainers.version>2.0.5</testcontainers.version>
  </properties>

  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>io.awspring.cloud</groupId>
        <artifactId>spring-cloud-aws-dependencies</artifactId>
        <version>${spring-cloud-aws.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
      <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>testcontainers-bom</artifactId>
        <version>${testcontainers.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

  <dependencies>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-jdbc</artifactId>
    </dependency>
    <dependency>
      <groupId>io.awspring.cloud</groupId>
      <artifactId>spring-cloud-aws-starter-sqs</artifactId>
    </dependency>
    <dependency>
      <groupId>org.postgresql</groupId>
      <artifactId>postgresql</artifactId>
      <scope>runtime</scope>
    </dependency>

    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-test</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>testcontainers-junit-jupiter</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>testcontainers-postgresql</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>testcontainers-localstack</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.awaitility</groupId>
      <artifactId>awaitility</artifactId>
      <version>4.3.0</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: Gerar o wrapper do Maven**

Run: `mvn wrapper:wrapper -Dmaven=3.9.16`
Expected: cria `mvnw`, `mvnw.cmd` e `.mvn/wrapper/maven-wrapper.properties`. A partir daqui, **todos** os comandos usam `./mvnw`.

- [ ] **Step 3: Escrever o teste que falha**

`src/test/java/com/nexuspay/worker/HealthEndpointTest.java`:

```java
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
```

O import de `LocalServerPort` é `org.springframework.boot.test.web.server.LocalServerPort` — verificado listando o conteúdo de `spring-boot-test-4.0.7.jar`.

- [ ] **Step 4: Rodar o teste e ver falhar**

Run: `./mvnw test -Dtest=HealthEndpointTest`
Expected: FAIL — a classe `WorkerApplication` ainda não existe.

- [ ] **Step 5: Criar a aplicação e a configuração**

`src/main/java/com/nexuspay/worker/WorkerApplication.java`:

```java
package com.nexuspay.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }
}
```

`src/main/java/com/nexuspay/worker/config/AwsConfig.java`:

```java
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
```

`src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: nexuspay-transaction-worker
  datasource:
    url: jdbc:postgresql://localhost:5433/nexuspay
    username: nexuspay
    password: nexuspay
  cloud:
    aws:
      region:
        static: us-east-1

management:
  endpoints:
    web:
      exposure:
        include: health
  endpoint:
    health:
      show-details: never

nexuspay:
  aws:
    profile: nexuspay-worker
  sqs:
    queue-url: https://sqs.us-east-1.amazonaws.com/797771596673/api-processar-transferencia-worker.fifo
```

A porta é **5433**, não 5432: máquinas Windows com serviço nativo `postgresql-x64-*` costumam ocupar a 5432, e o `docker-compose.yml` do gateway já mapeia 5433.

`src/test/resources/application-test.yml`:

```yaml
spring:
  cloud:
    aws:
      region:
        static: us-east-1
      credentials:
        access-key: test
        secret-key: test

nexuspay:
  sqs:
    queue-url: http://localhost:0/fila-que-nao-existe
    listener-enabled: false
```

Duas salvaguardas aqui:

- Sem `nexuspay.aws.profile`, `AwsConfig` não é ativado e nenhum teste toca credencial real.
- `listener-enabled: false` desliga o `@SqsListener` (Task 9) por padrão. Sem isso, **todo** teste que herda de `PostgresTestBase` subiria um consumidor apontado para uma URL inválida. Só o teste do listener religa a propriedade.

- [ ] **Step 6: Rodar o teste e ver passar**

Run: `./mvnw test -Dtest=HealthEndpointTest`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pom.xml mvnw mvnw.cmd .mvn src
git commit -m "feat: esqueleto do worker com health endpoint

Spring Boot 4.0.7 com Spring Cloud AWS 4.1.0 sobre Java 25. A versao do
Boot esta presa em 4.0.7 porque e a base do spring-cloud-build 5.0.2, sobre
o qual o Spring Cloud AWS 4.1.0 e construido.

Credencial AWS por perfil nomeado explicito, condicional em propriedade,
para nenhum teste conseguir tocar a conta real.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: `ledger_entries` no repositório do gateway

Esta tarefa acontece **inteira** em `c:\Users\ferna\Desktop\projects\nexus\nexuspay-api-gateway`, numa branch própria (`feat/fatia-2b-ledger-entries`). O Alembic é dono único do schema.

**Files:**
- Create: `app/domains/ledger/__init__.py`
- Create: `app/domains/ledger/models.py`
- Modify: `alembic/env.py:28` (adicionar um import)
- Create: `alembic/versions/<rev>_ledger_entries.py` (gerado)
- Test: `tests/integration/test_ledger_schema.py`

**Interfaces:**
- Produces: tabela `ledger_entries` com as colunas `id`, `transaction_id`, `account_id`, `direction`, `amount`, `balance_after`, `created_at`; enum PostgreSQL `ledger_direction` com valores `DEBIT` e `CREDIT`; constraint `uq_ledger_transaction_account`.

- [ ] **Step 1: Escrever o teste que falha**

`tests/integration/test_ledger_schema.py`:

```python
"""A tabela existe, a unica protege contra aplicacao dupla, e o CHECK impede
lancamento com valor nao positivo.

O worker da fatia 2b depende dessas tres garantias; nenhuma delas pode ser
verificada por teste do lado Java sem que exista aqui primeiro.
"""

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession


async def _semear(db_session: AsyncSession) -> tuple[uuid.UUID, uuid.UUID]:
    """Cria um usuario, uma conta e uma transacao PENDING, e devolve
    (transaction_id, account_id).
    """
    institution_id = (
        await db_session.execute(text("SELECT id FROM institutions LIMIT 1"))
    ).scalar_one()
    user_id = uuid.uuid4()
    await db_session.execute(
        text(
            "INSERT INTO users (id, full_name, email, document, password_hash) "
            "VALUES (:id, 'Dono Ledger', :email, :doc, 'x')"
        ),
        {"id": user_id, "email": f"ledger-{user_id.hex}@example.com",
         "doc": str(user_id.int)[:11]},
    )
    account_id = uuid.uuid4()
    await db_session.execute(
        text(
            "INSERT INTO accounts (id, owner_id, institution_id, branch, number, type) "
            "VALUES (:id, :owner, :inst, '0001', :num, 'CHECKING')"
        ),
        {"id": account_id, "owner": user_id, "inst": institution_id,
         "num": str(account_id.int)[:8]},
    )
    transaction_id = uuid.uuid4()
    await db_session.execute(
        text(
            "INSERT INTO transactions "
            "(id, type, destination_account_id, amount, idempotency_key, requested_by_user_id) "
            "VALUES (:id, 'DEPOSIT', :acct, 100.00, :key, :user)"
        ),
        {"id": transaction_id, "acct": account_id, "key": str(transaction_id),
         "user": user_id},
    )
    await db_session.flush()
    return transaction_id, account_id


async def _inserir(db_session, transaction_id, account_id, *, direction="CREDIT",
                   amount="100.00", balance_after="100.00"):
    await db_session.execute(
        text(
            "INSERT INTO ledger_entries "
            "(id, transaction_id, account_id, direction, amount, balance_after) "
            "VALUES (:id, :tx, :acct, CAST(:dir AS ledger_direction), :amt, :after)"
        ),
        {"id": uuid.uuid4(), "tx": transaction_id, "acct": account_id,
         "dir": direction, "amt": Decimal(amount), "after": Decimal(balance_after)},
    )
    await db_session.flush()


async def test_lancamento_valido_e_aceito(db_session: AsyncSession) -> None:
    transaction_id, account_id = await _semear(db_session)

    await _inserir(db_session, transaction_id, account_id)

    total = (
        await db_session.execute(
            text("SELECT count(*) FROM ledger_entries WHERE transaction_id = :tx"),
            {"tx": transaction_id},
        )
    ).scalar_one()
    assert total == 1


async def test_segundo_lancamento_na_mesma_conta_e_recusado(
    db_session: AsyncSession,
) -> None:
    """A unica e a ultima rede contra aplicacao dupla: uma transacao toca cada
    conta no maximo uma vez.
    """
    transaction_id, account_id = await _semear(db_session)
    await _inserir(db_session, transaction_id, account_id)

    with pytest.raises(IntegrityError):
        await _inserir(db_session, transaction_id, account_id)


async def test_valor_nao_positivo_e_recusado(db_session: AsyncSession) -> None:
    transaction_id, account_id = await _semear(db_session)

    with pytest.raises(IntegrityError):
        await _inserir(db_session, transaction_id, account_id, amount="0.00")


async def test_saldo_apos_negativo_e_recusado(db_session: AsyncSession) -> None:
    transaction_id, account_id = await _semear(db_session)

    with pytest.raises(IntegrityError):
        await _inserir(db_session, transaction_id, account_id, balance_after="-1.00")
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `uv run pytest tests/integration/test_ledger_schema.py -q`
Expected: FAIL — `relation "ledger_entries" does not exist`.

- [ ] **Step 3: Criar o modelo**

`app/domains/ledger/__init__.py`: arquivo vazio.

`app/domains/ledger/models.py`:

```python
import enum
import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Numeric,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.core.database import Base


class LedgerDirection(str, enum.Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


class LedgerEntry(Base):
    """Um movimento de saldo em uma conta, gravado pelo worker da fatia 2b.

    O gateway nunca escreve nesta tabela — ela existe aqui porque o Alembic e
    dono unico do schema, e uma tabela ausente do metadata seria proposta para
    DROP no proximo autogenerate.

    balance_after e o que torna a auditoria verificavel: o saldo passa a ser
    reconstruivel a partir do historico, e divergencia entre accounts.balance e
    o ultimo balance_after vira detectavel.
    """

    __tablename__ = "ledger_entries"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    transaction_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("transactions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    account_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("accounts.id", ondelete="RESTRICT"),
        nullable=False,
    )
    direction: Mapped[LedgerDirection] = mapped_column(
        Enum(LedgerDirection, name="ledger_direction"), nullable=False
    )
    amount: Mapped[Decimal] = mapped_column(Numeric(15, 2), nullable=False)
    balance_after: Mapped[Decimal] = mapped_column(Numeric(15, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint("amount > 0", name="check_ledger_positive_amount"),
        CheckConstraint("balance_after >= 0", name="check_ledger_non_negative_balance"),
        UniqueConstraint(
            "transaction_id", "account_id", name="uq_ledger_transaction_account"
        ),
        Index(
            "ix_ledger_account_created",
            "account_id",
            created_at.desc(),
            id.desc(),
        ),
    )
```

- [ ] **Step 4: Registrar o modelo no Alembic**

Em `alembic/env.py`, logo após a linha 27, adicionar:

```python
from app.domains.ledger.models import LedgerEntry  # noqa: F401
```

Sem isso, o `target_metadata` não conhece a tabela e o próximo `--autogenerate` proporia derrubá-la.

- [ ] **Step 5: Gerar e revisar a migration**

Run: `uv run alembic revision --autogenerate -m "ledger entries"`

Depois **abra o arquivo gerado e conserte o `downgrade()`**: o autogenerate emite `op.drop_table("ledger_entries")` mas **não** dropa o tipo enum. Sem isso, `downgrade` seguido de `upgrade` falha com `DuplicateObjectError` — foi exatamente o defeito corrigido em duas migrations da fatia 2a. O `downgrade()` deve terminar com:

```python
    op.execute("DROP TYPE IF EXISTS ledger_direction")
```

- [ ] **Step 6: Aplicar e rodar os testes**

```bash
uv run alembic upgrade head
$env:DATABASE_NAME="nexuspay_test"; uv run alembic upgrade head; $env:DATABASE_NAME="nexuspay"
uv run pytest tests/integration/test_ledger_schema.py -q
```

Expected: PASS nos quatro testes.

- [ ] **Step 7: Verificar o round-trip da migration**

```bash
uv run alembic downgrade -1
uv run alembic upgrade head
```

Expected: os dois comandos terminam sem erro. Se o segundo falhar com `DuplicateObjectError`, o `DROP TYPE` do Step 5 está faltando.

- [ ] **Step 8: Rodar a suíte inteira do gateway**

Run: `uv run pytest -q`
Expected: os 189 testes anteriores continuam passando, mais os 4 novos.

- [ ] **Step 9: Commit**

```bash
git add app/domains/ledger alembic tests/integration/test_ledger_schema.py
git commit -m "feat: tabela ledger_entries para a auditoria da fatia 2b

O gateway nunca escreve nesta tabela; ela vive aqui porque o Alembic e dono
unico do schema e uma tabela fora do metadata seria proposta para DROP no
proximo autogenerate.

O downgrade dropa o tipo enum explicitamente — sem isso, downgrade seguido de
upgrade falha com DuplicateObjectError, o mesmo defeito ja corrigido em duas
migrations da 2a.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: Schema de teste do worker e detecção de divergência

O schema mora no outro repositório. Esta tarefa versiona um dump e cria o teste que faz a divergência aparecer como falha, em vez de bug em produção.

**Files:**
- Create: `scripts/regenerate-schema.sh`
- Create: `src/test/resources/schema.sql` (gerado)
- Create: `src/test/java/com/nexuspay/worker/PostgresTestBase.java`
- Test: `src/test/java/com/nexuspay/worker/SchemaDriftTest.java`

**Interfaces:**
- Produces: classe base `PostgresTestBase`, que sobe PostgreSQL 16 via Testcontainers com `schema.sql` aplicado, e expõe `DataSource` e `JdbcClient` às subclasses via `@DynamicPropertySource`.

- [ ] **Step 1: Criar o script de regeneração**

`scripts/regenerate-schema.sh`:

```bash
#!/usr/bin/env bash
# Regenera src/test/resources/schema.sql a partir do banco do gateway.
#
# O Alembic no repositorio nexuspay-api-gateway e dono unico do schema. Este
# worker nao roda migration nenhuma, mas seus testes precisam das tabelas.
# Rode este script sempre que o gateway ganhar uma migration que toque
# accounts, transactions ou ledger_entries.
#
# Pre-requisito: o container nexuspay-postgres do gateway no ar, com
# `alembic upgrade head` ja aplicado.
set -euo pipefail

destino="$(dirname "$0")/../src/test/resources/schema.sql"

docker exec nexuspay-postgres pg_dump \
  -U nexuspay -d nexuspay \
  --schema-only --no-owner --no-privileges --no-comments \
  > "$destino"

echo "schema regravado em $destino ($(wc -l < "$destino") linhas)"
```

- [ ] **Step 2: Gerar o dump**

Run: `bash scripts/regenerate-schema.sh`
Expected: `src/test/resources/schema.sql` criado, contendo `CREATE TABLE public.ledger_entries`, `CREATE TABLE public.accounts` e `CREATE TABLE public.transactions`.

- [ ] **Step 3: Escrever a base de testes com Postgres**

`src/test/java/com/nexuspay/worker/PostgresTestBase.java`:

```java
package com.nexuspay.worker;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * PostgreSQL 16 descartavel, com o schema do gateway aplicado.
 *
 * A imagem e a mesma do docker-compose.yml do gateway: rodar teste contra uma
 * versao diferente da de producao esconde diferenca de comportamento em
 * exatamente as areas que importam aqui — trava de linha e RETURNING.
 *
 * O container e estatico e iniciado uma vez para toda a suite; o Testcontainers
 * o derruba no fim da JVM. Cada teste limpa o que criou.
 */
@SpringBootTest
@ActiveProfiles("test")
public abstract class PostgresTestBase {

    // Sem parametro generico: na Testcontainers 2.x,
    // org.testcontainers.postgresql.PostgreSQLContainer NAO e uma classe
    // generica — escrever PostgreSQLContainer<?> nao compila. A classe
    // generica antiga (org.testcontainers.containers.PostgreSQLContainer)
    // ainda existe no jar, mas e a forma legada.
    static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:16"))
                    .withDatabaseName("nexuspay")
                    .withUsername("nexuspay")
                    .withPassword("nexuspay")
                    .withInitScript("schema.sql");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void propriedades(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
}
```

- [ ] **Step 4: Escrever o teste de divergência**

`src/test/java/com/nexuspay/worker/SchemaDriftTest.java`:

```java
package com.nexuspay.worker;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * O schema vive no repositorio do gateway e chega aqui como dump versionado.
 * Este teste existe para que uma migration futura que renomeie ou remova uma
 * coluna de que o worker depende apareca como falha de teste, e nao como
 * excecao em producao.
 *
 * Quando ele falhar: rode scripts/regenerate-schema.sh e ajuste o worker.
 */
class SchemaDriftTest extends PostgresTestBase {

    @Autowired
    JdbcClient jdbc;

    private List<String> colunas(String tabela) {
        return jdbc.sql("""
                SELECT column_name FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = :tabela
                """)
                .param("tabela", tabela)
                .query(String.class)
                .list();
    }

    @Test
    void transactions_tem_as_colunas_que_o_worker_usa() {
        assertThat(colunas("transactions")).contains(
                "id", "type", "source_account_id", "destination_account_id",
                "amount", "status", "failure_reason", "updated_at");
    }

    @Test
    void accounts_tem_as_colunas_que_o_worker_usa() {
        assertThat(colunas("accounts")).contains("id", "balance", "status", "updated_at");
    }

    @Test
    void ledger_entries_tem_as_colunas_que_o_worker_grava() {
        assertThat(colunas("ledger_entries")).contains(
                "id", "transaction_id", "account_id", "direction",
                "amount", "balance_after", "created_at");
    }

    @Test
    void a_unica_do_ledger_existe() {
        var constraints = jdbc.sql("""
                SELECT conname FROM pg_constraint
                 WHERE conrelid = 'ledger_entries'::regclass AND contype = 'u'
                """).query(String.class).list();

        assertThat(constraints).contains("uq_ledger_transaction_account");
    }

    @Test
    void o_check_de_saldo_nao_negativo_das_contas_existe() {
        var constraints = jdbc.sql("""
                SELECT conname FROM pg_constraint
                 WHERE conrelid = 'accounts'::regclass AND contype = 'c'
                """).query(String.class).list();

        assertThat(constraints).contains("check_positive_balance");
    }
}
```

- [ ] **Step 5: Rodar e ver passar**

Run: `./mvnw test -Dtest=SchemaDriftTest`
Expected: PASS nos cinco testes. Docker precisa estar ligado.

- [ ] **Step 6: Provar que o teste detecta divergência**

Edite `src/test/resources/schema.sql` e renomeie a coluna `balance_after` para `saldo_apos` na definição de `ledger_entries`. Rode de novo.
Expected: FAIL em `ledger_entries_tem_as_colunas_que_o_worker_grava`. **Desfaça a edição** e confirme que volta a passar.

- [ ] **Step 7: Commit**

```bash
git add scripts src/test
git commit -m "test: schema do gateway aplicado em container e deteccao de divergencia

O schema vive no outro repositorio e chega aqui como dump versionado. O teste
de colunas existe para que uma migration futura que remova algo de que o
worker depende apareca como falha de teste, nao como excecao em producao.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: Domínio e `TransactionRepository`

**Files:**
- Create: `src/main/java/com/nexuspay/worker/domain/TransactionType.java`
- Create: `src/main/java/com/nexuspay/worker/domain/TransactionStatus.java`
- Create: `src/main/java/com/nexuspay/worker/domain/FailureReason.java`
- Create: `src/main/java/com/nexuspay/worker/domain/TransactionRecord.java`
- Create: `src/main/java/com/nexuspay/worker/persistence/TransactionRepository.java`
- Test: `src/test/java/com/nexuspay/worker/persistence/TransactionRepositoryTest.java`
- Create: `src/test/java/com/nexuspay/worker/Fixtures.java`

**Interfaces:**
- Consumes: `PostgresTestBase` da Task 3.
- Produces:
  - `TransactionRecord(UUID id, TransactionType type, UUID sourceAccountId, UUID destinationAccountId, BigDecimal amount, TransactionStatus status)`
  - `TransactionRepository.findForUpdate(UUID id) -> Optional<TransactionRecord>`
  - `TransactionRepository.markCompleted(UUID id) -> void`
  - `TransactionRepository.markFailed(UUID id, FailureReason reason) -> void`
  - `Fixtures.criarConta(JdbcClient, String saldo) -> UUID`
  - `Fixtures.criarTransferencia(JdbcClient, UUID origem, UUID destino, String valor) -> UUID`
  - `Fixtures.criarDeposito(JdbcClient, UUID destino, String valor) -> UUID`

- [ ] **Step 1: Escrever os tipos de domínio**

`domain/TransactionType.java`:

```java
package com.nexuspay.worker.domain;

public enum TransactionType {
    DEPOSIT,
    TRANSFER
}
```

`domain/TransactionStatus.java`:

```java
package com.nexuspay.worker.domain;

public enum TransactionStatus {
    PENDING,
    COMPLETED,
    FAILED
}
```

`domain/FailureReason.java`:

```java
package com.nexuspay.worker.domain;

/**
 * Conjunto fechado de codigos, nao texto livre: o frontend da fatia 3 traduz
 * por codigo, do mesmo jeito que ja faz com o envelope de erro do gateway.
 * Cabe em failure_reason VARCHAR(255).
 */
public enum FailureReason {
    INSUFFICIENT_FUNDS,
    SOURCE_ACCOUNT_UNAVAILABLE,
    DESTINATION_ACCOUNT_UNAVAILABLE
}
```

`domain/TransactionRecord.java`:

```java
package com.nexuspay.worker.domain;

import java.math.BigDecimal;
import java.util.UUID;

/** Uma linha de transactions. sourceAccountId e nulo em deposito. */
public record TransactionRecord(
        UUID id,
        TransactionType type,
        UUID sourceAccountId,
        UUID destinationAccountId,
        BigDecimal amount,
        TransactionStatus status) {}
```

- [ ] **Step 2: Escrever as fixtures de teste**

`src/test/java/com/nexuspay/worker/Fixtures.java`:

```java
package com.nexuspay.worker;

import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Semeia dados diretamente por SQL. O worker nao tem caminho de criacao de
 * conta nem de transacao — quem cria e o gateway, que e outro processo.
 */
public final class Fixtures {

    private Fixtures() {}

    public static UUID criarConta(JdbcClient jdbc, String saldo) {
        var institutionId = jdbc.sql("SELECT id FROM institutions LIMIT 1")
                .query(UUID.class).optional()
                .orElseGet(() -> criarInstituicao(jdbc));

        var ownerId = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO users (id, full_name, email, document, password_hash)
                VALUES (:id, 'Dono De Teste', :email, :doc, 'x')
                """)
                .param("id", ownerId)
                .param("email", "dono-" + ownerId + "@example.com")
                .param("doc", String.valueOf(Math.abs(ownerId.getLeastSignificantBits())).substring(0, 11))
                .update();

        var accountId = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO accounts (id, owner_id, institution_id, branch, number, type, balance)
                VALUES (:id, :owner, :inst, '0001', :num, 'CHECKING', :saldo)
                """)
                .param("id", accountId)
                .param("owner", ownerId)
                .param("inst", institutionId)
                .param("num", accountId.toString().substring(0, 8))
                .param("saldo", new BigDecimal(saldo))
                .update();
        return accountId;
    }

    private static UUID criarInstituicao(JdbcClient jdbc) {
        var id = UUID.randomUUID();
        jdbc.sql("INSERT INTO institutions (id, code, name) VALUES (:id, '001', 'Banco De Teste')")
                .param("id", id).update();
        return id;
    }

    public static UUID criarTransferencia(JdbcClient jdbc, UUID origem, UUID destino, String valor) {
        return inserirTransacao(jdbc, "TRANSFER", origem, destino, valor);
    }

    public static UUID criarDeposito(JdbcClient jdbc, UUID destino, String valor) {
        return inserirTransacao(jdbc, "DEPOSIT", null, destino, valor);
    }

    private static UUID inserirTransacao(
            JdbcClient jdbc, String tipo, UUID origem, UUID destino, String valor) {
        var id = UUID.randomUUID();
        var requester = jdbc.sql("SELECT owner_id FROM accounts WHERE id = :id")
                .param("id", destino).query(UUID.class).single();
        jdbc.sql("""
                INSERT INTO transactions
                  (id, type, source_account_id, destination_account_id, amount,
                   idempotency_key, requested_by_user_id)
                VALUES
                  (:id, CAST(:tipo AS transaction_type), :origem, :destino, :valor,
                   :chave, :user)
                """)
                .param("id", id)
                .param("tipo", tipo)
                .param("origem", origem)
                .param("destino", destino)
                .param("valor", new BigDecimal(valor))
                .param("chave", id.toString())
                .param("user", requester)
                .update();
        return id;
    }
}
```

Note o `CAST(:tipo AS transaction_type)`: sem ele o PostgreSQL recusa o parâmetro `varchar` numa coluna de enum.

- [ ] **Step 3: Escrever o teste que falha**

`src/test/java/com/nexuspay/worker/persistence/TransactionRepositoryTest.java`:

```java
package com.nexuspay.worker.persistence;

import com.nexuspay.worker.Fixtures;
import com.nexuspay.worker.PostgresTestBase;
import com.nexuspay.worker.domain.FailureReason;
import com.nexuspay.worker.domain.TransactionStatus;
import com.nexuspay.worker.domain.TransactionType;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
class TransactionRepositoryTest extends PostgresTestBase {

    @Autowired
    JdbcClient jdbc;

    @Autowired
    TransactionRepository repository;

    @Test
    void le_uma_transferencia_com_os_dois_lados() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var id = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        var encontrada = repository.findForUpdate(id).orElseThrow();

        assertThat(encontrada.type()).isEqualTo(TransactionType.TRANSFER);
        assertThat(encontrada.sourceAccountId()).isEqualTo(origem);
        assertThat(encontrada.destinationAccountId()).isEqualTo(destino);
        assertThat(encontrada.amount()).isEqualByComparingTo(new BigDecimal("100.00"));
        assertThat(encontrada.status()).isEqualTo(TransactionStatus.PENDING);
    }

    @Test
    void deposito_vem_com_origem_nula() {
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var id = Fixtures.criarDeposito(jdbc, destino, "100.00");

        var encontrada = repository.findForUpdate(id).orElseThrow();

        assertThat(encontrada.type()).isEqualTo(TransactionType.DEPOSIT);
        assertThat(encontrada.sourceAccountId()).isNull();
    }

    @Test
    void uuid_inexistente_devolve_vazio() {
        assertThat(repository.findForUpdate(UUID.randomUUID())).isEmpty();
    }

    @Test
    void marcar_concluida_grava_status_e_updated_at() {
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var id = Fixtures.criarDeposito(jdbc, destino, "100.00");

        repository.markCompleted(id);

        assertThat(repository.findForUpdate(id).orElseThrow().status())
                .isEqualTo(TransactionStatus.COMPLETED);
        assertThat(jdbc.sql("SELECT updated_at FROM transactions WHERE id = :id")
                .param("id", id).query(java.time.OffsetDateTime.class).optional())
                .isPresent();
    }

    @Test
    void marcar_falha_grava_status_e_motivo() {
        var origem = Fixtures.criarConta(jdbc, "0.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var id = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        repository.markFailed(id, FailureReason.INSUFFICIENT_FUNDS);

        assertThat(repository.findForUpdate(id).orElseThrow().status())
                .isEqualTo(TransactionStatus.FAILED);
        assertThat(jdbc.sql("SELECT failure_reason FROM transactions WHERE id = :id")
                .param("id", id).query(String.class).single())
                .isEqualTo("INSUFFICIENT_FUNDS");
    }
}
```

- [ ] **Step 4: Rodar e ver falhar**

Run: `./mvnw test -Dtest=TransactionRepositoryTest`
Expected: FAIL — `TransactionRepository` não existe.

- [ ] **Step 5: Implementar o repositório**

`src/main/java/com/nexuspay/worker/persistence/TransactionRepository.java`:

```java
package com.nexuspay.worker.persistence;

import com.nexuspay.worker.domain.FailureReason;
import com.nexuspay.worker.domain.TransactionRecord;
import com.nexuspay.worker.domain.TransactionStatus;
import com.nexuspay.worker.domain.TransactionType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class TransactionRepository {

    private final JdbcClient jdbc;

    public TransactionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Le a transacao travando a linha ate o fim da transacao de banco.
     *
     * E esta trava que fecha a reentrega, nao a checagem de status que vem
     * depois: dois consumidores com a mesma mensagem chegam aqui, o segundo
     * BLOQUEIA ate o primeiro commitar, e so entao le o status ja resolvido.
     * Sem o FOR UPDATE, os dois leriam PENDING e os dois aplicariam.
     */
    public Optional<TransactionRecord> findForUpdate(UUID id) {
        return jdbc.sql("""
                SELECT id, type, source_account_id, destination_account_id, amount, status
                  FROM transactions
                 WHERE id = :id
                 FOR UPDATE
                """)
                .param("id", id)
                .query(TransactionRepository::mapear)
                .optional();
    }

    public void markCompleted(UUID id) {
        atualizarStatus(id, TransactionStatus.COMPLETED, null);
    }

    public void markFailed(UUID id, FailureReason reason) {
        atualizarStatus(id, TransactionStatus.FAILED, reason.name());
    }

    private void atualizarStatus(UUID id, TransactionStatus status, String motivo) {
        // CAST explicito: o driver JDBC envia o parametro como varchar, e o
        // PostgreSQL nao converte varchar para enum em atribuicao.
        jdbc.sql("""
                UPDATE transactions
                   SET status = CAST(:status AS transaction_status),
                       failure_reason = :motivo,
                       updated_at = now()
                 WHERE id = :id
                """)
                .param("status", status.name())
                .param("motivo", motivo)
                .param("id", id)
                .update();
    }

    private static TransactionRecord mapear(ResultSet rs, int linha) throws SQLException {
        return new TransactionRecord(
                rs.getObject("id", UUID.class),
                TransactionType.valueOf(rs.getString("type")),
                rs.getObject("source_account_id", UUID.class),
                rs.getObject("destination_account_id", UUID.class),
                rs.getBigDecimal("amount"),
                TransactionStatus.valueOf(rs.getString("status")));
    }
}
```

- [ ] **Step 6: Rodar e ver passar**

Run: `./mvnw test -Dtest=TransactionRepositoryTest`
Expected: PASS nos cinco testes.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/nexuspay/worker/domain src/main/java/com/nexuspay/worker/persistence src/test
git commit -m "feat: leitura travada e escrita de status da transacao

O SELECT ... FOR UPDATE e o que fecha a reentrega: dois consumidores com a
mesma mensagem bloqueiam um no outro, e o segundo so le o status depois do
commit do primeiro.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: `AccountRepository` — o `UPDATE` condicional

Este é o núcleo da correção da fatia. Uma instrução faz a checagem e a escrita juntas, sem intervalo entre elas.

**Files:**
- Create: `src/main/java/com/nexuspay/worker/persistence/AccountRepository.java`
- Test: `src/test/java/com/nexuspay/worker/persistence/AccountRepositoryTest.java`

**Interfaces:**
- Consumes: `Fixtures`, `PostgresTestBase`.
- Produces:
  - `AccountRepository.debit(UUID accountId, BigDecimal amount) -> Optional<BigDecimal>` — o saldo resultante, ou vazio se não aplicou
  - `AccountRepository.credit(UUID accountId, BigDecimal amount) -> Optional<BigDecimal>`
  - `AccountRepository.isActive(UUID accountId) -> boolean`

- [ ] **Step 1: Escrever o teste que falha**

`src/test/java/com/nexuspay/worker/persistence/AccountRepositoryTest.java`:

```java
package com.nexuspay.worker.persistence;

import com.nexuspay.worker.Fixtures;
import com.nexuspay.worker.PostgresTestBase;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
class AccountRepositoryTest extends PostgresTestBase {

    @Autowired
    JdbcClient jdbc;

    @Autowired
    AccountRepository repository;

    private BigDecimal saldo(UUID conta) {
        return jdbc.sql("SELECT balance FROM accounts WHERE id = :id")
                .param("id", conta).query(BigDecimal.class).single();
    }

    @Test
    void debito_com_saldo_suficiente_devolve_o_saldo_resultante() {
        var conta = Fixtures.criarConta(jdbc, "500.00");

        var depois = repository.debit(conta, new BigDecimal("100.00"));

        assertThat(depois).isPresent();
        assertThat(depois.get()).isEqualByComparingTo(new BigDecimal("400.00"));
        assertThat(saldo(conta)).isEqualByComparingTo(new BigDecimal("400.00"));
    }

    @Test
    void debito_exatamente_igual_ao_saldo_e_permitido() {
        var conta = Fixtures.criarConta(jdbc, "100.00");

        var depois = repository.debit(conta, new BigDecimal("100.00"));

        assertThat(depois.orElseThrow()).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void debito_maior_que_o_saldo_nao_aplica_e_nao_mexe_no_saldo() {
        var conta = Fixtures.criarConta(jdbc, "50.00");

        var depois = repository.debit(conta, new BigDecimal("100.00"));

        assertThat(depois).isEmpty();
        assertThat(saldo(conta)).isEqualByComparingTo(new BigDecimal("50.00"));
    }

    @Test
    void debito_em_conta_encerrada_nao_aplica() {
        var conta = Fixtures.criarConta(jdbc, "500.00");
        jdbc.sql("UPDATE accounts SET status = 'CLOSED' WHERE id = :id")
                .param("id", conta).update();

        assertThat(repository.debit(conta, new BigDecimal("10.00"))).isEmpty();
        assertThat(saldo(conta)).isEqualByComparingTo(new BigDecimal("500.00"));
    }

    @Test
    void credito_em_conta_ativa_devolve_o_saldo_resultante() {
        var conta = Fixtures.criarConta(jdbc, "10.00");

        var depois = repository.credit(conta, new BigDecimal("90.00"));

        assertThat(depois.orElseThrow()).isEqualByComparingTo(new BigDecimal("100.00"));
    }

    @Test
    void credito_em_conta_encerrada_nao_aplica() {
        var conta = Fixtures.criarConta(jdbc, "0.00");
        jdbc.sql("UPDATE accounts SET status = 'CLOSED' WHERE id = :id")
                .param("id", conta).update();

        assertThat(repository.credit(conta, new BigDecimal("10.00"))).isEmpty();
        assertThat(saldo(conta)).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void conta_inexistente_nao_aplica_em_nenhum_dos_dois_sentidos() {
        var fantasma = UUID.randomUUID();

        assertThat(repository.debit(fantasma, new BigDecimal("1.00"))).isEmpty();
        assertThat(repository.credit(fantasma, new BigDecimal("1.00"))).isEmpty();
    }

    @Test
    void is_active_distingue_conta_ativa_de_encerrada_e_de_inexistente() {
        var ativa = Fixtures.criarConta(jdbc, "0.00");
        var encerrada = Fixtures.criarConta(jdbc, "0.00");
        jdbc.sql("UPDATE accounts SET status = 'CLOSED' WHERE id = :id")
                .param("id", encerrada).update();

        assertThat(repository.isActive(ativa)).isTrue();
        assertThat(repository.isActive(encerrada)).isFalse();
        assertThat(repository.isActive(UUID.randomUUID())).isFalse();
    }
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `./mvnw test -Dtest=AccountRepositoryTest`
Expected: FAIL — `AccountRepository` não existe.

- [ ] **Step 3: Implementar**

`src/main/java/com/nexuspay/worker/persistence/AccountRepository.java`:

```java
package com.nexuspay.worker.persistence;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * O unico escritor de accounts.balance no sistema inteiro.
 *
 * Os dois metodos abaixo fazem a checagem e a escrita numa unica instrucao,
 * sem intervalo entre elas. Isso ELIMINA o check-then-act em vez de proteger
 * com trava — o formato de defeito que apareceu quatro vezes no gateway.
 * Zero linhas afetadas ja e a resposta de negocio; nao ha leitura anterior em
 * que confiar.
 */
@Repository
public class AccountRepository {

    private final JdbcClient jdbc;

    public AccountRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Saldo resultante, ou vazio se a conta nao esta ativa ou nao tem saldo. */
    public Optional<BigDecimal> debit(UUID accountId, BigDecimal amount) {
        return jdbc.sql("""
                UPDATE accounts
                   SET balance = balance - :valor, updated_at = now()
                 WHERE id = :id AND status = 'ACTIVE' AND balance >= :valor
                RETURNING balance
                """)
                .param("valor", amount)
                .param("id", accountId)
                .query(BigDecimal.class)
                .optional();
    }

    /** Saldo resultante, ou vazio se a conta nao esta ativa ou nao existe. */
    public Optional<BigDecimal> credit(UUID accountId, BigDecimal amount) {
        return jdbc.sql("""
                UPDATE accounts
                   SET balance = balance + :valor, updated_at = now()
                 WHERE id = :id AND status = 'ACTIVE'
                RETURNING balance
                """)
                .param("valor", amount)
                .param("id", accountId)
                .query(BigDecimal.class)
                .optional();
    }

    /**
     * Diagnostico, nunca decisao: serve so para escolher entre
     * INSUFFICIENT_FUNDS e *_ACCOUNT_UNAVAILABLE depois que o UPDATE ja
     * decidiu, devolvendo zero linhas.
     */
    public boolean isActive(UUID accountId) {
        return jdbc.sql("SELECT 1 FROM accounts WHERE id = :id AND status = 'ACTIVE'")
                .param("id", accountId)
                .query(Integer.class)
                .optional()
                .isPresent();
    }
}
```

`status = 'ACTIVE'` é literal, não parâmetro: literal de string o PostgreSQL converte para enum sozinho, parâmetro `varchar` não.

- [ ] **Step 4: Rodar e ver passar**

Run: `./mvnw test -Dtest=AccountRepositoryTest`
Expected: PASS nos oito testes.

- [ ] **Step 5: Provar que a condição do saldo é real**

Remova temporariamente `AND balance >= :valor` do `debit` e rode de novo.
Expected: FAIL em `debito_maior_que_o_saldo_nao_aplica_e_nao_mexe_no_saldo` — e possivelmente também um erro do `CHECK (balance >= 0)` do banco, que é a rede de baixo. **Restaure a linha** e confirme que volta a passar.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/nexuspay/worker/persistence/AccountRepository.java src/test
git commit -m "feat: movimento de saldo por UPDATE condicional com RETURNING

Uma instrucao faz a checagem e a escrita juntas: zero linhas afetadas ja e a
resposta de negocio. Elimina o check-then-act em vez de proteger com trava.
RETURNING entrega o saldo resultante para o lancamento sem um SELECT extra.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 6: `LedgerRepository`

**Files:**
- Create: `src/main/java/com/nexuspay/worker/domain/LedgerDirection.java`
- Create: `src/main/java/com/nexuspay/worker/persistence/LedgerRepository.java`
- Test: `src/test/java/com/nexuspay/worker/persistence/LedgerRepositoryTest.java`

**Interfaces:**
- Produces:
  - `LedgerDirection` — `DEBIT`, `CREDIT`
  - `LedgerRepository.insert(UUID transactionId, UUID accountId, LedgerDirection direction, BigDecimal amount, BigDecimal balanceAfter) -> void`

- [ ] **Step 1: Escrever o enum**

`domain/LedgerDirection.java`:

```java
package com.nexuspay.worker.domain;

public enum LedgerDirection {
    DEBIT,
    CREDIT
}
```

- [ ] **Step 2: Escrever o teste que falha**

`src/test/java/com/nexuspay/worker/persistence/LedgerRepositoryTest.java`:

```java
package com.nexuspay.worker.persistence;

import com.nexuspay.worker.Fixtures;
import com.nexuspay.worker.PostgresTestBase;
import com.nexuspay.worker.domain.LedgerDirection;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Transactional
class LedgerRepositoryTest extends PostgresTestBase {

    @Autowired
    JdbcClient jdbc;

    @Autowired
    LedgerRepository repository;

    @Test
    void grava_o_lancamento_com_sentido_e_saldo_resultante() {
        var conta = Fixtures.criarConta(jdbc, "400.00");
        var tx = Fixtures.criarDeposito(jdbc, conta, "100.00");

        repository.insert(tx, conta, LedgerDirection.CREDIT,
                new BigDecimal("100.00"), new BigDecimal("400.00"));

        var sentido = jdbc.sql("""
                SELECT direction FROM ledger_entries
                 WHERE transaction_id = :tx AND account_id = :conta
                """)
                .param("tx", tx).param("conta", conta)
                .query(String.class).single();
        assertThat(sentido).isEqualTo("CREDIT");
    }

    @Test
    void a_mesma_transacao_pode_lancar_nas_duas_contas() {
        var origem = Fixtures.criarConta(jdbc, "400.00");
        var destino = Fixtures.criarConta(jdbc, "100.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        repository.insert(tx, origem, LedgerDirection.DEBIT,
                new BigDecimal("100.00"), new BigDecimal("400.00"));
        repository.insert(tx, destino, LedgerDirection.CREDIT,
                new BigDecimal("100.00"), new BigDecimal("100.00"));

        var total = jdbc.sql("SELECT count(*) FROM ledger_entries WHERE transaction_id = :tx")
                .param("tx", tx).query(Integer.class).single();
        assertThat(total).isEqualTo(2);
    }

    @Test
    void lancar_duas_vezes_na_mesma_conta_estoura_a_unica() {
        // Esta e a ultima rede contra aplicacao dupla. Se o SELECT FOR UPDATE
        // e a checagem de status falharem, e ela que impede o saldo de andar
        // duas vezes.
        var conta = Fixtures.criarConta(jdbc, "100.00");
        var tx = Fixtures.criarDeposito(jdbc, conta, "100.00");
        repository.insert(tx, conta, LedgerDirection.CREDIT,
                new BigDecimal("100.00"), new BigDecimal("100.00"));

        assertThatThrownBy(() -> repository.insert(tx, conta, LedgerDirection.CREDIT,
                new BigDecimal("100.00"), new BigDecimal("200.00")))
                .isInstanceOf(DuplicateKeyException.class);
    }
}
```

- [ ] **Step 3: Rodar e ver falhar**

Run: `./mvnw test -Dtest=LedgerRepositoryTest`
Expected: FAIL — `LedgerRepository` não existe.

- [ ] **Step 4: Implementar**

`src/main/java/com/nexuspay/worker/persistence/LedgerRepository.java`:

```java
package com.nexuspay.worker.persistence;

import com.nexuspay.worker.domain.LedgerDirection;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Auditoria do movimento de saldo.
 *
 * balance_after e o que torna o saldo verificavel: ele passa a ser
 * reconstruivel a partir do historico, e divergencia entre accounts.balance e
 * o ultimo balance_after vira detectavel.
 */
@Repository
public class LedgerRepository {

    private final JdbcClient jdbc;

    public LedgerRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(UUID transactionId, UUID accountId, LedgerDirection direction,
                       BigDecimal amount, BigDecimal balanceAfter) {
        jdbc.sql("""
                INSERT INTO ledger_entries
                  (id, transaction_id, account_id, direction, amount, balance_after)
                VALUES
                  (:id, :tx, :conta, CAST(:sentido AS ledger_direction), :valor, :depois)
                """)
                .param("id", UUID.randomUUID())
                .param("tx", transactionId)
                .param("conta", accountId)
                .param("sentido", direction.name())
                .param("valor", amount)
                .param("depois", balanceAfter)
                .update();
    }
}
```

- [ ] **Step 5: Rodar e ver passar**

Run: `./mvnw test -Dtest=LedgerRepositoryTest`
Expected: PASS nos três testes.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/nexuspay/worker/domain/LedgerDirection.java src/main/java/com/nexuspay/worker/persistence/LedgerRepository.java src/test
git commit -m "feat: gravacao dos lancamentos de auditoria

A unica (transaction_id, account_id) e a ultima rede contra aplicacao dupla:
se a trava e a checagem de status falharem, e ela que impede o saldo de andar
duas vezes.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 7: `TransactionProcessor` — ciclo de vida e SAVEPOINT

O coração da fatia. Aqui o SAVEPOINT deixa de ser teoria.

**Files:**
- Create: `src/main/java/com/nexuspay/worker/config/NestedTransactionConfig.java`
- Create: `src/main/java/com/nexuspay/worker/domain/BusinessFailure.java`
- Create: `src/main/java/com/nexuspay/worker/service/TransactionProcessor.java`
- Test: `src/test/java/com/nexuspay/worker/service/TransactionProcessorTest.java`

**Interfaces:**
- Consumes: `TransactionRepository`, `AccountRepository`, `LedgerRepository`.
- Produces: `TransactionProcessor.process(UUID transactionId) -> void`.

- [ ] **Step 1: Escrever o teste que falha**

`src/test/java/com/nexuspay/worker/service/TransactionProcessorTest.java`:

```java
package com.nexuspay.worker.service;

import com.nexuspay.worker.Fixtures;
import com.nexuspay.worker.PostgresTestBase;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sem @Transactional na classe: o processador abre a propria transacao, e um
 * teste transacional em volta transformaria o SAVEPOINT interno em algo
 * diferente do que roda em producao. Cada teste cria dados novos e nao limpa —
 * o container e descartado no fim da suite.
 */
class TransactionProcessorTest extends PostgresTestBase {

    @Autowired
    JdbcClient jdbc;

    @Autowired
    TransactionProcessor processor;

    private BigDecimal saldo(UUID conta) {
        return jdbc.sql("SELECT balance FROM accounts WHERE id = :id")
                .param("id", conta).query(BigDecimal.class).single();
    }

    private String status(UUID tx) {
        return jdbc.sql("SELECT status FROM transactions WHERE id = :id")
                .param("id", tx).query(String.class).single();
    }

    private String motivo(UUID tx) {
        return jdbc.sql("SELECT failure_reason FROM transactions WHERE id = :id")
                .param("id", tx).query(String.class).optional().orElse(null);
    }

    private int lancamentos(UUID tx) {
        return jdbc.sql("SELECT count(*) FROM ledger_entries WHERE transaction_id = :id")
                .param("id", tx).query(Integer.class).single();
    }

    @Test
    void transferencia_valida_move_saldo_e_grava_dois_lancamentos() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "100.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        processor.process(tx);

        assertThat(status(tx)).isEqualTo("COMPLETED");
        assertThat(saldo(origem)).isEqualByComparingTo(new BigDecimal("400.00"));
        assertThat(saldo(destino)).isEqualByComparingTo(new BigDecimal("200.00"));
        assertThat(lancamentos(tx)).isEqualTo(2);
    }

    @Test
    void deposito_valido_credita_e_grava_um_lancamento() {
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarDeposito(jdbc, destino, "250.00");

        processor.process(tx);

        assertThat(status(tx)).isEqualTo("COMPLETED");
        assertThat(saldo(destino)).isEqualByComparingTo(new BigDecimal("250.00"));
        assertThat(lancamentos(tx)).isEqualTo(1);
    }

    @Test
    void saldo_insuficiente_termina_em_failed_sem_mexer_em_nada() {
        var origem = Fixtures.criarConta(jdbc, "50.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        processor.process(tx);

        assertThat(status(tx)).isEqualTo("FAILED");
        assertThat(motivo(tx)).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(saldo(origem)).isEqualByComparingTo(new BigDecimal("50.00"));
        assertThat(saldo(destino)).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(lancamentos(tx)).isZero();
    }

    @Test
    void destino_encerrado_termina_em_failed() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");
        jdbc.sql("UPDATE accounts SET status = 'CLOSED' WHERE id = :id")
                .param("id", destino).update();

        processor.process(tx);

        assertThat(status(tx)).isEqualTo("FAILED");
        assertThat(motivo(tx)).isEqualTo("DESTINATION_ACCOUNT_UNAVAILABLE");
        assertThat(saldo(origem)).isEqualByComparingTo(new BigDecimal("500.00"));
    }

    @Test
    void origem_encerrada_termina_em_failed_com_motivo_proprio() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");
        jdbc.sql("UPDATE accounts SET status = 'CLOSED' WHERE id = :id")
                .param("id", origem).update();

        processor.process(tx);

        assertThat(status(tx)).isEqualTo("FAILED");
        assertThat(motivo(tx)).isEqualTo("SOURCE_ACCOUNT_UNAVAILABLE");
        assertThat(saldo(destino)).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void credito_aplicado_antes_do_debito_e_desfeito_quando_o_debito_falha() {
        // O teste do SAVEPOINT. Os UPDATE saem em ordem crescente de UUID, entao
        // com o destino de UUID menor o CREDITO acontece PRIMEIRO. Quando o
        // debito devolve zero linhas, o credito ja aplicado precisa sumir — e o
        // FAILED precisa sobreviver. Sem SAVEPOINT, ou o destino fica creditado
        // numa transferencia que falhou, ou a transacao volta para PENDING e a
        // varredura do gateway a republica para sempre.
        UUID destino;
        UUID origem;
        do {
            destino = Fixtures.criarConta(jdbc, "0.00");
            origem = Fixtures.criarConta(jdbc, "50.00");
        } while (destino.compareTo(origem) >= 0);

        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        processor.process(tx);

        assertThat(status(tx)).isEqualTo("FAILED");
        assertThat(motivo(tx)).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(saldo(destino))
                .as("o credito adiantado tem que ter sido desfeito")
                .isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(lancamentos(tx)).isZero();
    }

    @Test
    void uuid_inexistente_nao_faz_nada_e_nao_levanta() {
        processor.process(UUID.randomUUID());
    }

    @Test
    void transacao_ja_concluida_nao_e_aplicada_de_novo() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        processor.process(tx);
        processor.process(tx);

        assertThat(saldo(origem)).isEqualByComparingTo(new BigDecimal("400.00"));
        assertThat(saldo(destino)).isEqualByComparingTo(new BigDecimal("100.00"));
        assertThat(lancamentos(tx)).isEqualTo(2);
    }

    @Test
    void os_lancamentos_registram_o_saldo_resultante_de_cada_lado() {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "100.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        processor.process(tx);

        List<BigDecimal> saldos = jdbc.sql("""
                SELECT balance_after FROM ledger_entries
                 WHERE transaction_id = :tx ORDER BY direction
                """).param("tx", tx).query(BigDecimal.class).list();

        // ORDER BY direction: CREDIT antes de DEBIT em ordem alfabetica.
        assertThat(saldos.get(0)).isEqualByComparingTo(new BigDecimal("200.00"));
        assertThat(saldos.get(1)).isEqualByComparingTo(new BigDecimal("400.00"));
    }
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `./mvnw test -Dtest=TransactionProcessorTest`
Expected: FAIL — `TransactionProcessor` não existe.

- [ ] **Step 3: Criar o `TransactionTemplate` aninhado**

`src/main/java/com/nexuspay/worker/config/NestedTransactionConfig.java`:

```java
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
```

- [ ] **Step 4: Criar a exceção de negócio**

`src/main/java/com/nexuspay/worker/domain/BusinessFailure.java`:

```java
package com.nexuspay.worker.domain;

/**
 * Falha de regra, nao de infraestrutura.
 *
 * A distincao decide o destino da mensagem: falha de negocio termina a
 * transacao em FAILED e a mensagem e deletada; falha de infraestrutura sobe,
 * a mensagem nao e deletada e a SQS reentrega.
 */
public class BusinessFailure extends RuntimeException {

    private final FailureReason reason;

    public BusinessFailure(FailureReason reason) {
        super(reason.name());
        this.reason = reason;
    }

    public FailureReason reason() {
        return reason;
    }
}
```

- [ ] **Step 5: Implementar o processador**

`src/main/java/com/nexuspay/worker/service/TransactionProcessor.java`:

```java
package com.nexuspay.worker.service;

import com.nexuspay.worker.domain.BusinessFailure;
import com.nexuspay.worker.domain.FailureReason;
import com.nexuspay.worker.domain.LedgerDirection;
import com.nexuspay.worker.domain.TransactionRecord;
import com.nexuspay.worker.domain.TransactionStatus;
import com.nexuspay.worker.domain.TransactionType;
import com.nexuspay.worker.persistence.AccountRepository;
import com.nexuspay.worker.persistence.LedgerRepository;
import com.nexuspay.worker.persistence.TransactionRepository;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class TransactionProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransactionProcessor.class);

    private final TransactionRepository transactions;
    private final AccountRepository accounts;
    private final LedgerRepository ledger;
    private final TransactionTemplate nested;

    public TransactionProcessor(TransactionRepository transactions,
                                AccountRepository accounts,
                                LedgerRepository ledger,
                                TransactionTemplate nestedTransactionTemplate) {
        this.transactions = transactions;
        this.accounts = accounts;
        this.ledger = ledger;
        this.nested = nestedTransactionTemplate;
    }

    /** Um movimento de conta a aplicar, com o sentido. */
    private record Perna(UUID accountId, LedgerDirection direction) {}

    @Transactional
    public void process(UUID transactionId) {
        var encontrada = transactions.findForUpdate(transactionId);
        if (encontrada.isEmpty()) {
            // No-op TERMINAL. A fila e compartilhada entre ambientes e a
            // mensagem carrega so o UUID: um worker que nao acha a linha
            // recebeu mensagem de outro ambiente. Insistir faria a mensagem
            // voltar para sempre.
            log.info("transacao {} nao existe neste banco; descartando a mensagem",
                    transactionId);
            return;
        }

        var transacao = encontrada.get();
        if (transacao.status() != TransactionStatus.PENDING) {
            log.info("transacao {} ja esta {}; reentrega descartada",
                    transactionId, transacao.status());
            return;
        }

        try {
            nested.executeWithoutResult(status -> aplicar(transacao));
        } catch (BusinessFailure falha) {
            // O SAVEPOINT ja desfez qualquer movimento parcial; o FAILED
            // abaixo esta fora dele e sobrevive ao commit.
            log.info("transacao {} recusada: {}", transactionId, falha.reason());
            transactions.markFailed(transactionId, falha.reason());
            return;
        } catch (DuplicateKeyException duplicada) {
            // A unica do ledger disparou: outro consumidor ja aplicou esta
            // transacao. O status dele e a verdade; nao sobrescrevemos.
            log.warn("transacao {} ja possuia lancamento; nada aplicado", transactionId);
            return;
        }

        transactions.markCompleted(transactionId);
        log.info("transacao {} concluida", transactionId);
    }

    private void aplicar(TransactionRecord transacao) {
        if (transacao.type() == TransactionType.DEPOSIT) {
            aplicarPerna(transacao,
                    new Perna(transacao.destinationAccountId(), LedgerDirection.CREDIT));
            return;
        }

        // Ordem crescente de UUID: MessageGroupId e a conta de ORIGEM, entao
        // A->B e B->A caem em grupos diferentes, rodam em paralelo e tocam as
        // mesmas duas linhas. Sem ordenacao fixa, isso e deadlock.
        List<Perna> pernas = List.of(
                        new Perna(transacao.sourceAccountId(), LedgerDirection.DEBIT),
                        new Perna(transacao.destinationAccountId(), LedgerDirection.CREDIT))
                .stream()
                .sorted(Comparator.comparing(Perna::accountId))
                .toList();

        for (var perna : pernas) {
            aplicarPerna(transacao, perna);
        }
    }

    private void aplicarPerna(TransactionRecord transacao, Perna perna) {
        var resultado = perna.direction() == LedgerDirection.DEBIT
                ? accounts.debit(perna.accountId(), transacao.amount())
                : accounts.credit(perna.accountId(), transacao.amount());

        BigDecimal saldoDepois = resultado.orElseThrow(() -> new BusinessFailure(motivo(perna)));
        ledger.insert(transacao.id(), perna.accountId(), perna.direction(),
                transacao.amount(), saldoDepois);
    }

    /**
     * Diagnostico, nunca decisao: a decisao ja foi tomada pelo UPDATE, que
     * devolveu zero linhas. Isto so escolhe qual codigo gravar.
     */
    private FailureReason motivo(Perna perna) {
        if (perna.direction() == LedgerDirection.CREDIT) {
            return FailureReason.DESTINATION_ACCOUNT_UNAVAILABLE;
        }
        return accounts.isActive(perna.accountId())
                ? FailureReason.INSUFFICIENT_FUNDS
                : FailureReason.SOURCE_ACCOUNT_UNAVAILABLE;
    }
}
```

- [ ] **Step 6: Rodar e ver passar**

Run: `./mvnw test -Dtest=TransactionProcessorTest`
Expected: PASS nos nove testes.

- [ ] **Step 7: Provar que o SAVEPOINT é o que salva**

Em `TransactionProcessor.process`, troque `nested.executeWithoutResult(status -> aplicar(transacao))` por `aplicar(transacao)` — ou seja, sem savepoint. Rode de novo.
Expected: FAIL em `credito_aplicado_antes_do_debito_e_desfeito_quando_o_debito_falha`, porque o `markFailed` na mesma transação não desfaz o crédito. **Restaure** e confirme que volta a passar.

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/nexuspay/worker/config/NestedTransactionConfig.java src/main/java/com/nexuspay/worker/domain/BusinessFailure.java src/main/java/com/nexuspay/worker/service src/test
git commit -m "feat: ciclo de vida da transacao com SAVEPOINT

Os UPDATE saem em ordem crescente de UUID para nao dar deadlock com a
transferencia inversa, e isso significa que o credito pode sair antes do
debito. Quando o debito falha, o credito ja aplicado precisa sumir e o FAILED
precisa sobreviver — que e exatamente o que o SAVEPOINT permite.

O TransactionTemplate e programatico de proposito: @Transactional(NESTED) num
metodo da mesma classe nao passaria pelo proxy do Spring e viraria nada, em
silencio.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 8: Concorrência

Os cenários que só aparecem com duas conexões de verdade.

**Files:**
- Test: `src/test/java/com/nexuspay/worker/service/TransactionProcessorConcurrencyTest.java`

**Interfaces:**
- Consumes: `TransactionProcessor`, `Fixtures`, `PostgresTestBase`.
- Produces: nada de produção; esta tarefa só adiciona cobertura. Se algum teste falhar, a correção volta às Tasks 5 ou 7.

- [ ] **Step 1: Escrever os testes**

`src/test/java/com/nexuspay/worker/service/TransactionProcessorConcurrencyTest.java`:

```java
package com.nexuspay.worker.service;

import com.nexuspay.worker.Fixtures;
import com.nexuspay.worker.PostgresTestBase;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concorrencia real, com duas conexoes fisicas. Sem @Transactional: cada
 * thread precisa da propria transacao de banco, e um teste transacional em
 * volta serializaria tudo numa conexao so, escondendo exatamente o que
 * queremos provar.
 */
class TransactionProcessorConcurrencyTest extends PostgresTestBase {

    @Autowired
    JdbcClient jdbc;

    @Autowired
    TransactionProcessor processor;

    private BigDecimal saldo(UUID conta) {
        return jdbc.sql("SELECT balance FROM accounts WHERE id = :id")
                .param("id", conta).query(BigDecimal.class).single();
    }

    private List<String> statusDe(UUID... ids) {
        return List.of(ids).stream()
                .map(id -> jdbc.sql("SELECT status FROM transactions WHERE id = :id")
                        .param("id", id).query(String.class).single())
                .toList();
    }

    private void emParalelo(Runnable a, Runnable b) throws Exception {
        try (var pool = Executors.newFixedThreadPool(2)) {
            List<Callable<Void>> tarefas = List.of(
                    () -> { a.run(); return null; },
                    () -> { b.run(); return null; });
            for (var futuro : pool.invokeAll(tarefas)) {
                futuro.get();  // propaga qualquer excecao
            }
        }
    }

    @Test
    void duas_transferencias_de_100_contra_saldo_100_so_uma_passa() throws Exception {
        // O buraco que o gateway deixa aberto de proposito: as duas recebem 202
        // la, e o worker e quem tem que ser a autoridade.
        var origem = Fixtures.criarConta(jdbc, "100.00");
        var destinoA = Fixtures.criarConta(jdbc, "0.00");
        var destinoB = Fixtures.criarConta(jdbc, "0.00");
        var txA = Fixtures.criarTransferencia(jdbc, origem, destinoA, "100.00");
        var txB = Fixtures.criarTransferencia(jdbc, origem, destinoB, "100.00");

        emParalelo(() -> processor.process(txA), () -> processor.process(txB));

        assertThat(statusDe(txA, txB))
                .containsExactlyInAnyOrder("COMPLETED", "FAILED");
        assertThat(saldo(origem)).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void transferencias_inversas_concorrentes_concluem_sem_deadlock() throws Exception {
        var contaA = Fixtures.criarConta(jdbc, "500.00");
        var contaB = Fixtures.criarConta(jdbc, "500.00");
        var aParaB = Fixtures.criarTransferencia(jdbc, contaA, contaB, "100.00");
        var bParaA = Fixtures.criarTransferencia(jdbc, contaB, contaA, "100.00");

        emParalelo(() -> processor.process(aParaB), () -> processor.process(bParaA));

        assertThat(statusDe(aParaB, bParaA)).containsExactly("COMPLETED", "COMPLETED");
        assertThat(saldo(contaA)).isEqualByComparingTo(new BigDecimal("500.00"));
        assertThat(saldo(contaB)).isEqualByComparingTo(new BigDecimal("500.00"));
    }

    @Test
    void a_mesma_transacao_processada_em_paralelo_aplica_uma_vez_so() throws Exception {
        var origem = Fixtures.criarConta(jdbc, "500.00");
        var destino = Fixtures.criarConta(jdbc, "0.00");
        var tx = Fixtures.criarTransferencia(jdbc, origem, destino, "100.00");

        emParalelo(() -> processor.process(tx), () -> processor.process(tx));

        assertThat(saldo(origem)).isEqualByComparingTo(new BigDecimal("400.00"));
        assertThat(saldo(destino)).isEqualByComparingTo(new BigDecimal("100.00"));
        assertThat(jdbc.sql("SELECT count(*) FROM ledger_entries WHERE transaction_id = :id")
                .param("id", tx).query(Integer.class).single()).isEqualTo(2);
    }

    @Test
    void o_saldo_nunca_fica_negativo_sob_disputa() throws Exception {
        var origem = Fixtures.criarConta(jdbc, "150.00");
        var d1 = Fixtures.criarConta(jdbc, "0.00");
        var d2 = Fixtures.criarConta(jdbc, "0.00");
        var t1 = Fixtures.criarTransferencia(jdbc, origem, d1, "100.00");
        var t2 = Fixtures.criarTransferencia(jdbc, origem, d2, "100.00");

        emParalelo(() -> processor.process(t1), () -> processor.process(t2));

        assertThat(saldo(origem)).isGreaterThanOrEqualTo(BigDecimal.ZERO);
        assertThat(saldo(origem)).isEqualByComparingTo(new BigDecimal("50.00"));
    }
}
```

- [ ] **Step 2: Rodar**

Run: `./mvnw test -Dtest=TransactionProcessorConcurrencyTest`
Expected: PASS nos quatro testes.

Se `transferencias_inversas_concorrentes_concluem_sem_deadlock` falhar com `deadlock detected`, a ordenação por UUID em `TransactionProcessor.aplicar` está errada — corrija lá, não no teste.

Se `duas_transferencias_de_100_contra_saldo_100_so_uma_passa` falhar com saldo negativo, a condição `AND balance >= :valor` sumiu do `debit`.

- [ ] **Step 3: Provar que a ordenação é o que evita o deadlock**

Em `TransactionProcessor.aplicar`, remova o `.sorted(Comparator.comparing(Perna::accountId))`. Rode `transferencias_inversas_concorrentes_concluem_sem_deadlock` umas 5 vezes.
Expected: falha intermitente com `deadlock detected`. **Restaure** e confirme que passa de forma estável.

- [ ] **Step 4: Commit**

```bash
git add src/test/java/com/nexuspay/worker/service/TransactionProcessorConcurrencyTest.java
git commit -m "test: concorrencia real com duas conexoes

Cobre os quatro cenarios que so aparecem sob disputa: duas transferencias
contra o mesmo saldo, transferencias inversas (o caso de deadlock), a mesma
transacao em paralelo, e saldo nunca negativo.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 9: `@SqsListener` com LocalStack e DLQ

**Files:**
- Create: `src/main/java/com/nexuspay/worker/messaging/TransactionMessage.java`
- Create: `src/main/java/com/nexuspay/worker/messaging/TransactionListener.java`
- Test: `src/test/java/com/nexuspay/worker/messaging/TransactionListenerTest.java`

**Interfaces:**
- Consumes: `TransactionProcessor.process(UUID)`.
- Produces: `TransactionMessage(UUID transactionId)` com `@JsonProperty("transaction_id")`; listener ligado a `${nexuspay.sqs.queue-url}`.

- [ ] **Step 1: Escrever o record da mensagem**

`src/main/java/com/nexuspay/worker/messaging/TransactionMessage.java`:

```java
package com.nexuspay.worker.messaging;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * Corpo publicado pelo gateway: {"transaction_id": "<uuid>"}.
 *
 * O nome do campo e snake_case do lado Python. @JsonProperty explicito em vez
 * de estrategia global de nomes: e um contrato entre servicos, e deixa-lo
 * depender de configuracao global do Jackson faria uma mudanca nao relacionada
 * quebrar o consumo.
 */
public record TransactionMessage(@JsonProperty("transaction_id") UUID transactionId) {}
```

- [ ] **Step 2: Escrever o teste que falha**

`src/test/java/com/nexuspay/worker/messaging/TransactionListenerTest.java`:

```java
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

        var dlq = sqs.createQueue(CreateQueueRequest.builder()
                .queueName("teste-dlq.fifo")
                .attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true"))
                .build()).queueUrl();
        var dlqArn = sqs.getQueueAttributes(b -> b.queueUrl(dlq)
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
}
```

- [ ] **Step 3: Rodar e ver falhar**

Run: `./mvnw test -Dtest=TransactionListenerTest`
Expected: FAIL — não há listener; o status nunca sai de `PENDING` e o `await` estoura.

- [ ] **Step 4: Implementar o listener**

`src/main/java/com/nexuspay/worker/messaging/TransactionListener.java`:

```java
package com.nexuspay.worker.messaging;

import com.nexuspay.worker.service.TransactionProcessor;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Ponto de entrada unico do worker. Nao ha API de negocio.
 *
 * O modo de confirmacao padrao (ON_SUCCESS) e exatamente a semantica que o
 * spec pede e por isso nao e sobrescrito: retorno normal deleta a mensagem,
 * excecao NAO deleta e a SQS reentrega ate a redrive policy mandar para a DLQ.
 * Todo caminho terminal — inclusive falha de negocio e UUID desconhecido —
 * retorna normalmente de proposito.
 *
 * Condicional em propriedade porque os testes de banco herdam de
 * PostgresTestBase e subiriam um consumidor apontado para uma URL invalida.
 * Ligado por padrao (matchIfMissing): producao nao precisa configurar nada, e
 * so o ambiente de teste desliga.
 */
@Component
@ConditionalOnProperty(name = "nexuspay.sqs.listener-enabled",
        havingValue = "true", matchIfMissing = true)
class TransactionListener {

    private final TransactionProcessor processor;

    TransactionListener(TransactionProcessor processor) {
        this.processor = processor;
    }

    @SqsListener("${nexuspay.sqs.queue-url}")
    void aoReceber(TransactionMessage mensagem) {
        processor.process(mensagem.transactionId());
    }
}
```

- [ ] **Step 5: Rodar e ver passar**

Run: `./mvnw test -Dtest=TransactionListenerTest`
Expected: PASS nos três testes.

- [ ] **Step 6: Rodar a suíte inteira**

Run: `./mvnw test`
Expected: todos os testes passam.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/nexuspay/worker/messaging src/test/java/com/nexuspay/worker/messaging
git commit -m "feat: listener da fila FIFO

O modo de confirmacao padrao ja e a semantica do spec e por isso nao e
sobrescrito: retorno normal deleta, excecao nao deleta e a SQS reentrega ate a
DLQ. Todo caminho terminal retorna normalmente de proposito.

Testes contra LocalStack: a fila real e compartilhada com producao e nenhum
teste automatizado pode toca-la.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 10: Teste contra a fila real, e documentação

**Files:**
- Create: `src/test/java/com/nexuspay/worker/RealQueueSmokeTest.java`
- Modify: `README.md`
- Create: `docs/superpowers/follow-ups-fatia-2b.md`

**Interfaces:**
- Consumes: perfil `nexuspay-worker` em `~/.aws/credentials`.
- Produces: nada de produção.

- [ ] **Step 1: Escrever o teste marcado**

`src/test/java/com/nexuspay/worker/RealQueueSmokeTest.java`:

```java
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
```

- [ ] **Step 2: Rodar com credenciais**

Run: `./mvnw test -Dtest=RealQueueSmokeTest -Dfila.real=true`
Expected: PASS nos dois testes.

- [ ] **Step 3: Confirmar que ele se pula sozinho**

Run: `./mvnw test -Dtest=RealQueueSmokeTest`
Expected: os dois testes aparecem como *skipped*.

- [ ] **Step 4: Escrever o README**

Substituir `README.md` por:

````markdown
# nexuspay-transaction-worker

Consumidor da fila SQS FIFO do NexusPay. Executa o movimento de saldo com
garantias ACID e grava a auditoria. **Não tem API de negócio** — a única porta
HTTP é `/actuator/health`, que o gateway usa contra o scale-to-zero.

Java 25, Spring Boot 4.0.7, Spring Cloud AWS 4.1.0, acesso a dados por
`JdbcClient`.

## Rodar

Pré-requisitos: Docker ligado, e o Postgres do gateway no ar em `localhost:5433`
com `alembic upgrade head` aplicado.

```bash
./mvnw spring-boot:run
```

## Testes

```bash
./mvnw test
```

A suíte sobe PostgreSQL 16 e LocalStack em containers descartáveis. **Nenhum
teste automatizado fala com a fila real.**

O teste de fumaça contra a AWS de verdade fica desligado por padrão e só lê
atributos — não publica nem consome:

```bash
./mvnw test -Dtest=RealQueueSmokeTest -Dfila.real=true
```

> **A fila é uma só, compartilhada entre desenvolvimento e produção.** Nunca
> aponte este worker para ela com um banco de desenvolvimento a menos que
> entenda a consequência: uma mensagem de produção consumida aqui não acha o
> UUID no banco local, é descartada como no-op terminal, e a transação real
> fica presa em `PENDING` até a varredura do gateway republicar.

## Schema

Este worker **não roda migrations**. O Alembic no repositório
`nexuspay-api-gateway` é dono único do schema.

Os testes usam um dump versionado em `src/test/resources/schema.sql`. Depois de
qualquer migration nova no gateway que toque `accounts`, `transactions` ou
`ledger_entries`:

```bash
bash scripts/regenerate-schema.sh
```

`SchemaDriftTest` falha se uma coluna de que o worker depende desaparecer.

## Credenciais

Perfil nomeado em `~/.aws/credentials`, nunca no código nem em `.env`:

```ini
[nexuspay-worker]
aws_access_key_id = ...
aws_secret_access_key = ...
```

A policy desse usuário precisa de exatamente cinco ações sobre o ARN da fila
principal: `ReceiveMessage`, `DeleteMessage`, `GetQueueAttributes`,
`GetQueueUrl`, `ChangeMessageVisibility`. Nada sobre a DLQ — quem move a
mensagem é a própria SQS.
````

- [ ] **Step 5: Escrever os follow-ups**

`docs/superpowers/follow-ups-fatia-2b.md`, registrando o que ficou em aberto:

```markdown
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

### `sqs:CreateQueue` está concedido nos dois usuários IAM

Contraria a separação de privilégio pretendida — `backend-fastapi-producer`
deveria apenas enviar mensagem. Verificado por sondagem durante o desenho da 2b.

## Fora de escopo, mas alguém vai perguntar

- **Estorno de transação já concluída.** Não existe caminho. Provavelmente é uma
  transação nova em sentido inverso, com referência à original, e não um
  `UPDATE` na linha antiga.
- **Reprocessamento manual do que caiu na DLQ.** Hoje é trabalho de console.
```

- [ ] **Step 6: Rodar a suíte inteira uma última vez**

Run: `./mvnw test`
Expected: tudo passa; `RealQueueSmokeTest` aparece como *skipped*.

- [ ] **Step 7: Commit**

```bash
git add src/test/java/com/nexuspay/worker/RealQueueSmokeTest.java README.md docs
git commit -m "docs: README, follow-ups e teste de fumaca contra a fila real

O teste real so LE atributos, nunca publica nem consome: na 2a um teste que
consumia da fila compartilhada chegou a deixar transferencias reais
invisiveis.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Verificação final da fatia

Antes de fechar a branch, confirmar cada critério de aceitação do spec §14 contra um teste real:

| # | Critério | Coberto por |
|---|---|---|
| 1 | Transferência válida → `COMPLETED`, saldos corretos, 2 lançamentos | Task 7, `transferencia_valida_move_saldo_e_grava_dois_lancamentos` |
| 2 | Depósito válido → 1 lançamento | Task 7, `deposito_valido_credita_e_grava_um_lancamento` |
| 3 | Sem saldo → `FAILED` / `INSUFFICIENT_FUNDS` | Task 7, `saldo_insuficiente_termina_em_failed_sem_mexer_em_nada` |
| 4 | Destino encerrado → `DESTINATION_ACCOUNT_UNAVAILABLE` | Task 7, `destino_encerrado_termina_em_failed` |
| 5 | Mesma mensagem duas vezes aplica uma vez | Task 7 e Task 8 |
| 6 | Duas de R$100 contra R$100 | Task 8, `duas_transferencias_de_100_contra_saldo_100_so_uma_passa` |
| 7 | A→B e B→A sem deadlock | Task 8, `transferencias_inversas_concorrentes_concluem_sem_deadlock` |
| 7a | Crédito adiantado desfeito (SAVEPOINT) | Task 7, `credito_aplicado_antes_do_debito_e_desfeito_quando_o_debito_falha` |
| 8 | UUID inexistente descartado | Task 7 e Task 9 |
| 9 | Falha de infra não deleta a mensagem | Task 9 (modo de confirmação padrão) |
| 10 | Após N recebimentos vai para a DLQ | **lacuna conhecida** — ver abaixo |
| 11 | `balance` nunca negativo | Task 8, `o_saldo_nunca_fica_negativo_sob_disputa` |

**Lacuna declarada no critério 10.** A Task 9 configura a redrive policy no LocalStack com `maxReceiveCount = 2`, mas nenhum teste força uma exceção de infraestrutura repetida para observar a mensagem chegar na DLQ. Fechar isso exige injetar uma falha no `TransactionProcessor` (por exemplo, um bean de teste que lança nas duas primeiras chamadas) e depois consultar a DLQ. Se o tempo permitir, acrescente esse teste na Task 9; caso contrário, registre-o em `follow-ups-fatia-2b.md` como cobertura ausente — **não** o declare coberto.
