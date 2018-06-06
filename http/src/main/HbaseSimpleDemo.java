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
        write();
        System.out.println();
        System.out.println();
        get();
    }

    public static void write(){
        CloseableHttpClient httpClient = HttpClients.createDefault();
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("business", "case_test"));
        params.add(new BasicNameValuePair("keyId", "hbase_zq"));
        params.add(new BasicNameValuePair("timestamp", "1528084116"));
        params.add(new BasicNameValuePair("value", "hello"));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(params, Charset.forName("utf8"));
        HttpPost httpPost = new HttpPost("http://controlcenter.ds.sina.com.cn/waic/hbase/case/insert");
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

    public static String get(){
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet("http://controlcenter.ds.sina.com.cn/waic/hbase/case/query?business=case_test&keyId=hbase_zq&timestamp=1528084116");
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
}
