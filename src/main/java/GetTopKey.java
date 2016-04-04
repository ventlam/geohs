/**
 * Created by vent on 4/4/16.
 * Get top 10 search keyword of 660 city in china
 *
 */
import org.elasticsearch.node.NodeBuilder.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.action.get.GetResponse;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class GetTopKey {

    public static void main(String[] args) {
        Client client = null;
        //http://123.59.87.5/
        try {
            byte[] esip = new byte[]{(byte)123,(byte)59,(byte)87,(byte)5};
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", "dw_elk").build();

             client = TransportClient.builder().settings(settings).build().
                     addTransportAddress(new InetSocketTransportAddress(InetAddress.getByAddress(esip), 9300));

            GetResponse response = client.prepareGet("dbuser", "usertype", "1090414").get();


        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        client.close();
    }
}
