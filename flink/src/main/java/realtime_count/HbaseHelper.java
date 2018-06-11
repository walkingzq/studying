package realtime_count;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Zhao Qing on 2018/6/9.
 */
public class HbaseHelper {

    public static void insert(String business, String keyId, long timestamp, String value){
        CloseableHttpClient httpClient = HttpClients.createDefault();
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("business", business));
        params.add(new BasicNameValuePair("keyId", keyId));
        params.add(new BasicNameValuePair("timestamp", String.valueOf(timestamp)));
        params.add(new BasicNameValuePair("value", value));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(params, Charset.forName("utf8"));
        HttpPost httpPost = new HttpPost("http://controlcenter.ds.sina.com.cn/waic/hbase/case/insert");
        httpPost.setEntity(entity);
        CloseableHttpResponse httpResponse = null;
        try {
            httpResponse = httpClient.execute(httpPost);
            HttpEntity httpEntity = httpResponse.getEntity();
            System.out.println("insert res:" + EntityUtils.toString(httpEntity));//打印返回结果
        }catch (IOException exc){
            exc.printStackTrace();
        }finally {
            try {
                httpClient.close();
                httpResponse.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String query(String business, String keyId, String timestamp){
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet("http://controlcenter.ds.sina.com.cn/waic/hbase/case/query?business=" + business + "&keyId=" + keyId + "&timestamp=" + timestamp);
        CloseableHttpResponse httpResponse = null;
        String res = null;
        try {
            httpResponse = httpClient.execute(httpGet);
            HttpEntity httpEntity = httpResponse.getEntity();
            res = EntityUtils.toString(httpEntity);//获得返回结果
        }catch (IOException exc){
            exc.printStackTrace();
        }finally {
            try {
                httpClient.close();
                httpResponse.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }
}
