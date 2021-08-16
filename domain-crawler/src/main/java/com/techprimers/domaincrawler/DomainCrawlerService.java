package com.techprimers.domaincrawler;
import datadog.trace.api.Trace;

import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import datadog.trace.api.DDTags;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

@Service
public class DomainCrawlerService {

  private KafkaTemplate<String, Domain> kafkaTemplate;
  private final String KAFKA_TOPIC = "web-domains";

  public DomainCrawlerService(KafkaTemplate<String, Domain> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @Trace(operationName = "crawl.service", resourceName = "DomainCrawlerService.crawl")
  public void crawl(String name) {

      // Tags can be set when creating the span
	  
    Mono<DomainList> domainListMono = WebClient.create()
        .get()
        .uri("https://api.domainsdb.info/v1/domains/search?domain=" + name + "&zone=com")
        .accept(MediaType.APPLICATION_JSON)
        .retrieve()
        .bodyToMono(DomainList.class);

    domainListMono.subscribe(domainList -> {

      domainList.domains
          .forEach(domain -> {
              Tracer tracer = GlobalTracer.get();
              Span span = tracer.buildSpan("crawl.service")
                      .withTag(DDTags.SERVICE_NAME, "domain-crawler")
                      .withTag(DDTags.RESOURCE_NAME, "DomainCrawlerService.crawl")
                      .start();
                  try (Scope scope = tracer.activateSpan(span)) {
                      // Tags can also be set after creation
                      span.setTag("domain", domain.getDomain());

                      // The code you’re tracing

                  } catch (Exception e) {
                      // Set error on span
                  } finally {
                      // Close span in a finally block
                      span.finish();
                  }

            kafkaTemplate.send(KAFKA_TOPIC, domain);
            System.out.println("Domain message" + domain.getDomain());
          });
    });

  }
  
}
