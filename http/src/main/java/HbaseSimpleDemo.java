import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create by Zhao Qing on 2018/6/4
 */
public class HbaseSimpleDemo {
    public static void main(String[] args) throws Exception{
        String res = query("experimental_results", "realtime_index_count", "1528686600");
        System.out.println(res);
//        System.out.println("starting...");
//        write();
//        System.out.println();
//        System.out.println();
//        get();
//        System.out.println();
//        System.out.println("ending....");
    }

    public synchronized static void write(){
        CloseableHttpClient httpClient = HttpClients.createDefault();
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("business", "experimental_results"));
        params.add(new BasicNameValuePair("keyId", "hbase_zq"));
        params.add(new BasicNameValuePair("timestamp", "1528084116"));
        params.add(new BasicNameValuePair("value", "hello"));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(params, Charset.forName("utf8"));
        HttpPost httpPost = new HttpPost("http://10.77.29.74:8080/waic/hbase/case/insert");
        httpPost.setEntity(entity);
        CloseableHttpResponse httpResponse = null;
        try {
            httpResponse = httpClient.execute(httpPost);
            HttpEntity httpEntity = httpResponse.getEntity();
            System.out.println(EntityUtils.toString(httpEntity));
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

    public synchronized static String get(){
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet("http://10.77.29.74:8080/waic/hbase/case/query?business=experimental_results&keyId=hbase_zq&timestamp=1528084116");
        CloseableHttpResponse httpResponse = null;
        String res = null;
        try {
            httpResponse = httpClient.execute(httpGet);
            HttpEntity httpEntity = httpResponse.getEntity();
            System.out.println(EntityUtils.toString(httpEntity));
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
        return "test";
    }

    public static String query(String business, String keyId, String timestamp){
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet("http://10.77.29.74:8080/waic/hbase/case/query?business=" + business + "&keyId=" + keyId + "&timestamp=" + timestamp);
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
