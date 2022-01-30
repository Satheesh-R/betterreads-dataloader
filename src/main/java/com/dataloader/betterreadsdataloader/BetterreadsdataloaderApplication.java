package com.dataloader.betterreadsdataloader;

import com.dataloader.betterreadsdataloader.author.Author;
import com.dataloader.betterreadsdataloader.author.AuthorRepository;
import com.dataloader.betterreadsdataloader.book.Book;
import com.dataloader.betterreadsdataloader.book.BookRepository;
import com.dataloader.betterreadsdataloader.connection.DataStaxAstraProperties;
import lombok.extern.slf4j.Slf4j;
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

import javax.annotation.PostConstruct;
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

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
@Slf4j
public class BetterreadsdataloaderApplication {

	@Autowired
	private AuthorRepository authorRepository;
	@Autowired
	private BookRepository bookRepository;
	@Value("${datadump.location.author}")
	private String authorDumpLocation;
	@Value("${datadump.location.works}")
	private  String worksDumpLocation;
	public static void main(String[] args) {
		SpringApplication.run(BetterreadsdataloaderApplication.class, args);
	}

	@PostConstruct
	public void start() throws JSONException, IOException {
		initAuthors();
		initWorks();
	}

	private void initAuthors() throws IOException {
		Path path = Paths.get(authorDumpLocation);
		try(Stream<String> authorFileLines = Files.lines(path)){
			authorFileLines.forEach( line -> {
				String authorDetails = line.substring(line.indexOf("{"));
				JSONObject authorJson = null;
				try {
					authorJson = new JSONObject(authorDetails);
				} catch (JSONException jsonException) {
					jsonException.printStackTrace();
				}

				Author author = new Author();
				author.setPenName(authorJson.optString("name"));
				author.setName(authorJson.optString("personal_name"));
				author.setId(authorJson.optString("key").replace("/authors/",""));
				log.info("Saving author details -> {}",author);
				authorRepository.save(author);
			});
		}
		catch(IOException ioexception){
			 throw ioexception;
		}
	}

	private void initWorks() throws IOException {
		Path path = Paths.get(worksDumpLocation);
		try(Stream<String> worksFileLines = Files.lines(path)){
			worksFileLines.forEach(line -> {
				String workDetails = line.substring(line.indexOf("{"));
				JSONObject workJson = null;
				try {
					workJson = new JSONObject(workDetails);
					JSONObject descriptionObj = workJson.optJSONObject("description");
					JSONObject publishedObj = workJson.optJSONObject("created");
					JSONArray coversArr = workJson.optJSONArray("covers");
					JSONArray authorArr = workJson.optJSONArray("authors");
					Book book = new Book();
					book.setId(workJson.getString("key").replace("/works/",""));
					book.setName(workJson.optString("title"));
					if(descriptionObj != null)
						book.setDescription(descriptionObj.optString("value"));
					if(publishedObj != null)
						book.setPublishedDate(LocalDate.parse(publishedObj.optString("value"),
								DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")));
					if(coversArr != null){
						List<String> coverIds = new ArrayList<>();
						for(int i = 0;i< coversArr.length();i++){
							coverIds.add(coversArr.optString(i));
						}
						book.setCoverIds(coverIds);
					}
					if(authorArr != null){
						List<String> authorIds = new ArrayList<>();
						for(int i = 0;i < authorArr.length();i++) {
							authorIds.add(authorArr.getJSONObject(i).getJSONObject("author").getString("key")
									.replace("/authors/", ""));
						}
						List<String> authorNames = authorIds.stream().map(authorId -> authorRepository.findById(authorId))
								.map(optionalAuthor -> {
									if(!optionalAuthor.isPresent())
										return "Unknown Author";
									else
										return optionalAuthor.get().getPenName();
								}).collect(Collectors.toList());
						book.setAuthorIds(authorIds);
						book.setAuthorNames(authorNames);
						log.info("Saving book details -> {}",book);
						bookRepository.save(book);
					}
				} catch (JSONException jsonException) {
					jsonException.printStackTrace();
				}
			});
		}
	}

	@Bean
	public CqlSessionBuilderCustomizer cqlSessionBuilderCustomizer(DataStaxAstraProperties dataStaxAstraProperties){
		Path bundlePath = dataStaxAstraProperties.getSecureConnectBundle().toPath();
		return cqlSessionBuilder -> cqlSessionBuilder.withCloudSecureConnectBundle(bundlePath);
	}
}
