package com.iictbuet.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import com.iictbuet.betterreadsdataloader.author.Author;
import com.iictbuet.betterreadsdataloader.author.AuthorRepository;
import com.iictbuet.betterreadsdataloader.book.Book;
import com.iictbuet.betterreadsdataloader.book.BookRepository;
import com.iictbuet.betterreadsdataloader.connection.DataStaxAstraPropertie;

import org.json.JSONArray;
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
    @Autowired
    BookRepository bookRepository;

    @Value("${datadump.location.author}")
    private String authorDataDumpLoacation;

    @Value("${datadump.location.works}")
    private String worksDataDumpLoacation;

    public static void main(String[] args) {
        SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
    }

    public void initAuthor() {
        Path paths = Paths.get(authorDataDumpLoacation);
        try (Stream<String> lines = Files.lines(paths)) {
            lines.forEach(line -> {
                // Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    // Construct Author object
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));
                    System.out.println("Saving auther ......" + author.getName());
                    // Persist using reporitory
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

    public void initWorks() {
        Path paths = Paths.get(worksDataDumpLoacation);
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try (Stream<String> lines = Files.lines(paths)) {
            lines.forEach(line -> {
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    // Construct Book object
                    Book book = new Book();
                    book.setId(jsonObject.getString("key").replace("/works/", ""));
                    book.setName(jsonObject.optString("title"));
                    JSONObject descriptionObj = jsonObject.optJSONObject("description");
                    if (descriptionObj != null) {
                        book.setDescription(descriptionObj.optString("value"));
                    }
                    JSONObject publishObj = jsonObject.optJSONObject("created");
                    if (publishObj != null) {
                        String date = publishObj.getString("value");
                        book.setPublishedDate(LocalDate.parse(date, dateFormat));
                    }

                    JSONArray coversObj = jsonObject.optJSONArray("covers");
                    if (coversObj != null) {
                        List<String> coverIds = new ArrayList<>();
                        for (int i = 0; i < coversObj.length(); i++) {
                            coverIds.add(coversObj.getString(i));
                        }
                        book.setCoverIds(coverIds);
                    }

                    JSONArray authorArr = jsonObject.optJSONArray("authors");
                    if (authorArr != null) {
                        List<String> authorIds = new ArrayList<>();
                        for (int i = 0; i < coversObj.length(); i++) {
                            String authorId = authorArr.getJSONObject(i).getJSONObject("author").optString("key")
                                    .replace("/authors/", "");
                            authorIds.add(authorId);
                        }
                        book.setAuthorId(authorIds);
                        List<String> authorName = authorIds.stream().map(id -> authorRepository.findById(id))
                                .map(optionalAuthor -> {
                                    if (!optionalAuthor.isPresent())
                                        return "Unknown Author";
                                    return optionalAuthor.get().getName();
                                }).collect(Collectors.toList());
                        book.setAuthorName(authorName);

                    }
                    System.out.println("Saving Book ......" + book.getName());
                    // Persist using reporitory
                    bookRepository.save(book);

                } catch (JSONException e) {
                    // TODO: handle exception
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void start() {
        // initAuthor();
        initWorks();
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
