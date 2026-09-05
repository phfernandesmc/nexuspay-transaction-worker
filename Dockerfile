# Ver DEPLOY.md na raiz do workspace para o roteiro completo de deploy.

# Estagio de build: precisa do JDK completo e do Maven baixado pelo wrapper.
FROM eclipse-temurin:25-jdk AS build
WORKDIR /app

COPY .mvn .mvn
COPY mvnw pom.xml ./
# chmod explicito: o bit de execucao do mvnw ja se perdeu uma vez neste
# repositorio ao ser reescrito (git o guardava como 100644 antes de ser
# corrigido), e nada garante que um checkout futuro preserve o modo do
# arquivo. Custa uma linha; nao custar teria custado um build quebrado.
RUN chmod +x mvnw && ./mvnw -q dependency:go-offline -B

# Fonte por ultimo: as dependencias (camada mais cara) ficam em cache
# entre builds enquanto so o codigo mudar.
COPY src src
RUN ./mvnw -q package -DskipTests -B

# Estagio final: so o JRE e o jar empacotado, sem Maven nem codigo-fonte.
FROM eclipse-temurin:25-jre AS runtime
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

RUN useradd --create-home --shell /usr/sbin/nologin appuser
USER appuser

EXPOSE 8080

# -Dserver.port, e nao editar application.yml: Render injeta PORT em
# runtime e espera o processo escutar nela — a unica porta HTTP do worker e
# /actuator/health, alvo do wake_worker do gateway, entao sem isto o health
# check do host falharia contra a porta errada. Forma shell no ENTRYPOINT
# e obrigatoria para ${PORT} expandir.
ENTRYPOINT ["sh", "-c", "java -Dserver.port=${PORT:-8080} -jar app.jar"]
