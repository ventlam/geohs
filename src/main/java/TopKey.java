/**
 * Created by vent on 4/4/16.
 * Get top 10 key word for each city in china
 * Using Jest Client
 */
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import jdk.nashorn.api.scripting.JSObject;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TopKey {


    public  static JsonArray getAggsFromES(String esHost,List<String> hostList,String query,String indexName)
    {
        //"http://123.59.87.5:9200"
        HttpClientConfig clientConfig = new HttpClientConfig.Builder(esHost)
                .multiThreaded(true).build();
        HttpClientConfig clientConfig1 = new HttpClientConfig.Builder(esHost).addServer(hostList).readTimeout(200000).multiThreaded(true).build();

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(clientConfig);
        JestClient jestClient = factory.getObject();
        JsonArray topKey =null;
        // Get get = new Get.Builder("logstash-2016.04.04", "AVPfH4tOiuQlOo93vPrz").type("rs").build();
        try {

            Search search = new Search.Builder(query)
                    // multiple index or types can be added.
                    .addIndex(indexName)
                    .build();

            SearchResult result = jestClient.execute(search);
            topKey = result.getJsonObject().getAsJsonObject("aggregations").getAsJsonObject("top_key").getAsJsonArray("buckets");


           // System.out.println("------------------");

            // System.out.println(result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }

        jestClient.shutdownClient();
        return topKey;
    }
    public static HashMap<String,HashMap<String,Double>> JsonTranform(JsonArray topKey)
    {
        HashMap<String,HashMap<String,Double>> citykeyMap =  new HashMap<String,HashMap<String,Double>>();
        for (int i = 0; i < topKey.size(); i++)
        {

            JsonObject cityKey =  topKey.get(i).getAsJsonObject();
            String cityName =  cityKey.get("key").getAsJsonPrimitive().getAsString();
            JsonArray keyArr = cityKey.getAsJsonObject("top_city_keyword").getAsJsonArray("buckets");
            HashMap<String,Double> keyMap =  new HashMap<String,Double>();
            for(int j=0; j< keyArr.size();j++)
            {

                String keyName =  keyArr.get(j).getAsJsonObject().get("key").getAsJsonPrimitive().getAsString();
                //System.out.println(keyName);
                double keyCount = keyArr.get(j).getAsJsonObject().get("doc_count").getAsDouble();
                keyMap.put(keyName,keyCount);
            }
            //System.out.println(keyMap.toString());
            citykeyMap.put(cityName,keyMap);
        }
        //System.out.println(citykeyMap.toString());

        return citykeyMap;

    }

    public static void main(String[] args)
    {

        String indexName = "logstash-2016.04.04";
        String esHost = "http://123.59.87.5:9200";
        List<String> hostList = new ArrayList<>();
        hostList.add("http://123.59.87.35:9200");
        String query = "{\n" +
                "    \"size\": 0,\n" +
                "    \"query\": {\n" +
                "        \"bool\" : {\n" +
                "            \"must\" : [{\n" +
                "                \"term\" : {\n" +
                "                    \"u_ac\" : \"search\"\n" +
                "                }\n" +
                "            },\n" +
                "            {\n" +
                "            \"exists\" : {\n" +
                "                \"field\" : \"u_key\"\n" +
                "            }\n" +
                "        }]\n" +
                "    }\n" +
                "    },\n" +
                "    \"aggs\": {\n" +
                "     \"top_key\" : {\n" +
                "      \"terms\" : {\n" +
                "       \"field\" : \"u_city.raw\",\n" +
                "       \"size\" : 0\n" +
                "       },\n" +
                "       \"aggs\" : {\n" +
                "        \"top_city_keyword\" : {\n" +
                "         \"terms\" : {\n" +
                "       \"field\" : \"u_key.raw\"\n" +
                "        }\n" +
                "       }\n" +
                "        }\n" +
                "       }\n" +
                "       }\n" +
                "}";
        JsonArray ja = TopKey.getAggsFromES(esHost, hostList, query, indexName);
      HashMap<String,HashMap<String,Double>> cityKeyMap = TopKey.JsonTranform(ja);
       System.out.println(cityKeyMap.toString());

    }
}