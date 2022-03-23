package com.iictbuet.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import com.iictbuet.betterreadsdataloader.author.Author;
import com.iictbuet.betterreadsdataloader.author.AuthorRepository;
import com.iictbuet.betterreadsdataloader.connection.DataStaxAstraPropertie;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;


@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraPropertie.class)
public class BetterreadsDataLoaderApplication {

   @Autowired
   AuthorRepository authorRepository;

   @Value("${datadump.location.author}")
   private String authorDataDumpLoacation;

   @Value("${datadump.location.works}")
   private String worksDataDumpLoacation;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}

    @PostConstruct
    public void start(){
        Path paths = Paths.get(authorDataDumpLoacation);
        try(Stream<String>lines = Files.lines(paths)) {
            lines.forEach(line->{
                //Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                //Construct Author object
                Author author = new Author();
                author.setName(jsonObject.optString("name"));
                author.setPersonalName(jsonObject.optString("personal_name"));
                author.setId(jsonObject.optString("key").replace("/authors/", ""));
                System.out.println("Saving auther ......"+author.getName());
                //Persist using reporitory
                authorRepository.save(author);
                } catch (JSONException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
           

            });
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
	 /**
     * This is necessary to have the Spring Boot app use the Astra secure bundle 
     * to connect to the database
     */
	
	@Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraPropertie astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
   

}
