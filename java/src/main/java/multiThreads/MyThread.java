package multiThreads;

/**
 * Created by Zhao Qing on 2018/5/27.
 */
public class MyThread extends Thread{
    private IdGenerater idGenerater;

    private String name;

    public MyThread(){}

    public MyThread(IdGenerater idGenerater, String name){
        this.idGenerater = idGenerater;
        this.name = name;
    }

    @Override
    public void run(){
        try {
            Thread.sleep(1000);
        }catch (InterruptedException exc){

        }
        synchronized (idGenerater){
            System.out.println(name + ": " + idGenerater.getNextId());
            try {
                idGenerater.wait();
            }catch (InterruptedException exc){

            }
        }
    }
}
