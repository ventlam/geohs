import com.google.gson.JsonArray;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * Created by vent on 4/5/16.
 */
public class GenHotSearchKey {

    static String  yesterday = DateUtil.getYesterdayDateString();
    public static void genCityKey()
    {

        String indexName = "logstash-"+yesterday;
        System.out.println(indexName);
        String esHost = "http://123.59.87.5:9200";
        List<String> hostList = new ArrayList<>();
        hostList.add("http://123.59.87.35:9200");
        hostList.add("http://123.59.139.172:9200");
        hostList.add("http://123.59.58.26:9200");
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
                "       \"field\" : \"u_city.raw\"\n" +
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
        RedisIO.zaddCityKey(cityKeyMap);

    }
    public static void main(String[] args)
    {
        GenHotSearchKey.genCityKey();
    }
}
