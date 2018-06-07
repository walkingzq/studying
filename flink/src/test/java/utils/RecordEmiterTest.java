package utils;

import org.junit.Test;
import realtime_count.SimpleDemo;

import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Zhao Qing on 2018/5/14.
 */
public class RecordEmiterTest {

    private static final Pattern p_insert_type = Pattern.compile("(insert_type:(\\d*))");

    private static final Pattern p_luicode = Pattern.compile("(luicode:(\\d*))");

    private static final Pattern p_uicode = Pattern.compile("(uicode:(\\d*))");


    public void job() throws Exception {
        new RecordEmiter().job(100);
    }

    public static void main(String[] args) throws Exception{
        String str = "2018-06-06 16:08:28\t117.174.156.70\t2736320532\t14000116\t419697\t1\t1\t4247161350829104\t2135487301\tdomain_id=>4247161350829104,is_auto=>0,spr=>from:1084395010;wm:9856_0004;luicode:10000002;uicode:10000002;ext:insert_type:5|oriuicode:10000001_10000002;aid:01AqF3wDdryf8HoERGNNUeGx1z8cjSDMW_DRbVmnX8QDCY-XU.;lang:zh_CN;networktype:wifi;featurecode:10000001;ua:vivo-vivo Y31__weibo__8.4.3__android__android5.1.1,object_type=>comment,mid=>4247160558023726,object_id=>4247161350829104";
        long time = toTimestamp(str);
        SimpleDemo.WindowWordCountEvent wwce = new SimpleDemo.WindowWordCountEvent(time, time, SimpleDemo.getRecordType(str), 1);
        System.out.println(wwce.toString());
        System.out.println();
        System.out.println(wwce.toJson());
        String[] strs = str.split("\\t");
        String extend = strs[9];
        System.out.println(extend);
//        Matcher m1 = p_insert_type.matcher(extend);
//        if (m1.find()){
//            System.out.println(m1.group(1));
//            System.out.println(m1.group(2));
//        }
//
//        Matcher m2 = p_luicode.matcher(extend);
//        if (m2.find()){
//            System.out.println(m2.group(1));
//            System.out.println(m2.group(2));
//        }
//
//        Matcher m3 = p_uicode.matcher(extend);
//        if (m3.find()){
//            System.out.println(m3.group(1));
//            System.out.println(m3.group(2));
//        }

    }



    public static long toTimestamp(String str){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        try {
            date = simpleDateFormat.parse(str);
        }catch (ParseException exc){
            exc.printStackTrace();
        }
        return date.getTime() / 1000;
    }
}