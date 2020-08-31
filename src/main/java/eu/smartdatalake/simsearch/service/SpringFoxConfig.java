package eu.smartdatalake.simsearch.service;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Configuration of Swagger using a Docket bean for exposing the documentation of the REST API requests supported by the back-end service.
 */
@Configuration
@EnableSwagger2
public class SpringFoxConfig {                                    
    @Bean
    public Docket api() { 
        return new Docket(DocumentationType.SWAGGER_2)  
		.select()
		.apis(RequestHandlerSelectors.basePackage("eu.smartdatalake.simsearch.service"))
		.paths(PathSelectors.any()).build();                                         
    }
}
