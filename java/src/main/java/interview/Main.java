package interview;

/**
 * Created by Zhao Qing on 2018/5/27.
 */
public class Main {

    public static void main(String[] args){
        String str = "I am a student.";
        System.out.println(reverse(str));
    }

    public static String reverse(String str){
        StringBuilder sb = new StringBuilder();
        String[] strs = str.split(" ");
        for (int i = strs.length - 1; i >= 0; i--){
            sb.append(strs[i]);
            if (i != 0){sb.append(" ");}
        }
        return sb.toString();
    }
}
