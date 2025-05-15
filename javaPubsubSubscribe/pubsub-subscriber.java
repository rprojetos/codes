// pom.xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.1.0</version>
        <relativePath/>
    </parent>
    <groupId>com.example</groupId>
    <artifactId>pubsub-subscriber</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>pubsub-subscriber</name>
    <description>Google PubSub Subscriber com Spring Boot</description>

    <properties>
        <java.version>17</java.version>
        <spring-cloud-gcp.version>4.1.4</spring-cloud-gcp.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <dependency>
            <groupId>com.google.cloud</groupId>
            <artifactId>spring-cloud-gcp-starter-pubsub</artifactId>
            <version>${spring-cloud-gcp.version}</version>
        </dependency>

        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>

        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </exclude>
                    </excludes>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>

// application.yml
spring:
  cloud:
    gcp:
      project-id: seu-projeto-id
      credentials:
        location: classpath:sua-credencial.json
  pubsub:
    subscriber:
      retry:
        # Configurações de retry para o subscriber
        max-attempts: 5
        initial-retry-delay: 100ms
        max-retry-delay: 10s
        retry-delay-multiplier: 2.0

# Nome da subscription que será usada
app:
  pubsub:
    subscription: nome-da-sua-subscription

// src/main/java/com/example/pubsubsubscriber/PubsubSubscriberApplication.java
package com.example.pubsubsubscriber;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PubsubSubscriberApplication {

    public static void main(String[] args) {
        SpringApplication.run(PubsubSubscriberApplication.class, args);
    }
}

// src/main/java/com/example/pubsubsubscriber/config/PubSubConfig.java
package com.example.pubsubsubscriber.config;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.AckMode;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;

@Configuration
public class PubSubConfig {

    @Value("${app.pubsub.subscription}")
    private String subscriptionName;

    @Bean
    public MessageChannel pubsubInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public PubSubInboundChannelAdapter messageChannelAdapter(
            PubSubTemplate pubSubTemplate,
            MessageChannel pubsubInputChannel) {
        
        PubSubInboundChannelAdapter adapter = new PubSubInboundChannelAdapter(
                pubSubTemplate, subscriptionName);
        adapter.setOutputChannel(pubsubInputChannel);
        adapter.setAckMode(AckMode.MANUAL);
        adapter.setPayloadType(String.class);
        return adapter;
    }
}

// src/main/java/com/example/pubsubsubscriber/model/MensagemJson.java
package com.example.pubsubsubscriber.model;

import lombok.Data;
import java.time.LocalDateTime;

@Data
public class MensagemJson {
    private String id;
    private String conteudo;
    private LocalDateTime timestamp;
    private String tipo;
    // Adicione outros campos conforme necessário
}

// src/main/java/com/example/pubsubsubscriber/service/PubSubMessageListener.java
package com.example.pubsubsubscriber.service;

import com.example.pubsubsubscriber.model.MensagemJson;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;
import com.google.cloud.spring.pubsub.support.GcpPubSubHeaders;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class PubSubMessageListener {

    private final ObjectMapper objectMapper;

    @ServiceActivator(inputChannel = "pubsubInputChannel")
    public void receberMensagem(Message<String> message) {
        log.info("Mensagem recebida: {}", message.getPayload());
        
        // Extrair a mensagem PubSub para realizar ACK
        BasicAcknowledgeablePubsubMessage originalMessage = 
            message.getHeaders().get(GcpPubSubHeaders.ORIGINAL_MESSAGE, 
                BasicAcknowledgeablePubsubMessage.class);
        
        try {
            // Converter payload JSON para o objeto MensagemJson
            MensagemJson mensagemJson = objectMapper.readValue(
                message.getPayload(), MensagemJson.class);
            
            // Processar a mensagem recebida
            processarMensagem(mensagemJson);
            
            // Confirmar que a mensagem foi processada com sucesso
            if (originalMessage != null) {
                originalMessage.ack();
            }
        } catch (Exception e) {
            log.error("Erro ao processar mensagem: {}", e.getMessage(), e);
            
            // Em caso de erro, não confirma o recebimento (NACK)
            // Isso fará com que a mensagem seja reentregue conforme as políticas de retry
            if (originalMessage != null) {
                originalMessage.nack();
            }
        }
    }
    
    private void processarMensagem(MensagemJson mensagem) {
        // Implementar a lógica de processamento da mensagem aqui
        log.info("Processando mensagem: ID={}, Tipo={}, Conteúdo={}", 
            mensagem.getId(), mensagem.getTipo(), mensagem.getConteudo());
        
        // Aqui você pode adicionar qualquer lógica de negócio necessária
        // Por exemplo, salvar no banco de dados, chamar outros serviços, etc.
    }
}

// src/main/java/com/example/pubsubsubscriber/config/JacksonConfig.java
package com.example.pubsubsubscriber.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return objectMapper;
    }
}
