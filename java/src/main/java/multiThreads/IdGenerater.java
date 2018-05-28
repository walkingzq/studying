package multiThreads;

/**
 * Created by Zhao Qing on 2018/5/27.
 */
public class IdGenerater {
    private int id;

    public IdGenerater(int id){
        this.id = id;
    }

    public int getNextId(){
        return id++;
    }

    public int getId(){
        return id;
    }
}
