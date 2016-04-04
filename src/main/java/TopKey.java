/**
 * Created by vent on 4/4/16.
 * Get top 10 key word for each city in china
 * Using Jest Client
 */
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class TopKey {


    public static void main(String[] args) {

    HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://123.59.87.5:9200")
            .multiThreaded(true).build();
    JestClientFactory factory = new JestClientFactory();
    factory.setHttpClientConfig(clientConfig);
    JestClient jestClient = factory.getObject();

       // Get get = new Get.Builder("logstash-2016.04.04", "AVPfH4tOiuQlOo93vPrz").type("rs").build();

        try {
            String query = "{\n" +
                    "    \"query\": {\n" +
                    "        \"filtered\" : {\n" +
                    "            \"query\" : {\n" +
                    "                \"query_string\" : {\n" +
                    "                    \"query\" : \"test\"\n" +
                    "                }\n" +
                    "            },\n" +
                    "            \"filter\" : {\n" +
                    "                \"term\" : { \"user\" : \"kimchy\" }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}";

            Search search = new Search.Builder(query)
                    // multiple index or types can be added.
                    .addIndex("logstash-2016.04.04")
                    .build();

            SearchResult result = jestClient.execute(search);
            System.out.println(result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }


        jestClient.shutdownClient();


    }
}