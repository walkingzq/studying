import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Zhao Qing on 2018/5/2.
 */
public class CommonTest {
    @Test
    public void CollectionsSingletonListTest(){
        String str = "hello";
        //Collections.singletonList()方法会生成一个只包含一个元素的只读list，
        // 对该list进行读以外的其他操作都会抛出java.lang.UnsupportedOperationException
        List<String> list = Collections.singletonList(str);
        Iterator iterator = list.iterator();
        while (iterator.hasNext()){
            System.out.println(iterator.next());
        }
//        list.add("hh");//会抛出java.lang.UnsupportedOperationException异常
//        list.set(0, "h");//会抛出java.lang.UnsupportedOperationException异常
    }
}
