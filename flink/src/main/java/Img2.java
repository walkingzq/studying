/**
 * Created by Zhao Qing on 2018/5/15.
 */
public class Img2 {
    public static void main(String[] args) throws Exception{
        int count = Integer.MIN_VALUE;
        int delay = args[0] == null ? 10 : Integer.parseInt(args[0]);
        while (true){
            for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE; i++){
                count++;
            }
            try{
                Thread.sleep(delay);
            }catch (InterruptedException exc){
                throw new InterruptedException(exc.getMessage());
            }

        }
    }
}
