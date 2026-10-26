## Verificar que todo este instalado
```bash
java -version
```

```bash
curl --version
```

```bash
unzip -v
```

# Inicializar proyecto de springboot
## 1. Descargar proyecto base (en zip)
```bash
curl -fL --get "https://start.spring.io/starter.zip" \--data-urlencode "type=maven-project" \--data-urlencode "language=java" \
 --data-urlencode "bootVersion=4.1.1" \
 --data-urlencode "groupId=com.minipedidos" \
 --data-urlencode "artifactId=minipedidos" \
 --data-urlencode "name=minipedidos" \
 --data-urlencode "packageName=com.minipedidos" \
 --data-urlencode "packaging=jar" \
 --data-urlencode "javaVersion=17" \
 --data-urlencode "dependencies=web,rabbitmq,validation" \
 -o /tmp/minipedidos-spring.zip
```
## 2. Descomprime el zip
```bash
unzip -o /tmp/minipedidos-spring.zip -d /workspaces/minipedidos
```
## 3. Dar permisos de ejecución a maven wrapper
```bash
chmod +x mvnw
```
## 4. Verificar que maven funciona
```bash
./mvnw -version
```
## 5. Compilar proyecto
```bash
./mvnw clean package -DskipTests
```
Resultado esperado:
`[INFO] BUILD SUCCESS`
Esto confirma que el proyecto Spring Boot se generó correctamente y puede compilarse
## 6. Ejecutar
```bash
./mvnw spring-boot:run
```
Deberías observar:
`Tomcat started on port 8080
Started MiniPedidosApplication`
En esta etapa todavía no tenemos configurados RabbitMQ, los productores, los consumidores ni el 
formulario HTML. Es normal que el navegador muestre un error `404` si intentas acceder a la raíz de 
la aplicación.

Cuando comprobemos que inicia correctamente, detendremos Spring Boot con `Ctrl + C`

Al llegar hasta aqui ya deberias tener:
* Un proyecto Spring Boot generado completamente mediante comandos.
* Maven Wrapper configurado.
* Dependencias para desarrollar la API REST y comunicarnos con RabbitMQ.
* Código fuente inicial de la aplicación.
* Compilación comprobada con BUILD SUCCESS.

_Una consideración importante para evitar los errores anteriores: si Spring Initializr genera Spring 
Boot 4, utilizaremos JacksonJsonMessageConverter en RabbitMQConfig.java, no Jackson2JsonMessageConverter, que corresponde a versiones anteriores de Spring AMQP_

El siguiente paso es crear `docker-compose.yml` y levantar un único nodo 
RabbitMQ, exactamente como funcionó en nuestro laboratorio original.

# Adicionales

## Hot reload
añadir la dependencia en el `pom.xml`
```bash
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-devtools</artifactId>
    <optional>true</optional>
</dependency>
```
configurar el `aplication.properties`
```bash
# Desactivar la caché de recursos estáticos
spring.web.resources.cache.period=0

# Desactivar la caché si utilizas motores de plantillas (Thymeleaf, Freemarker, etc.)
spring.thymeleaf.cache=false
```