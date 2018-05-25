package checkingout.delay;

/**
 * Created by Zhao Qing on 2018/5/24.
 */
public class DelayEvent {
    private String value;

    private long in_time;

    private long out_time;

    private int count;

    public DelayEvent(){}

    public DelayEvent(String value, long in_time, long out_time, int count) {
        this.value = value;
        this.in_time = in_time;
        this.out_time = out_time;
        this.count = count;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getIn_time() {
        return in_time;
    }

    public void setIn_time(long in_time) {
        this.in_time = in_time;
    }

    public long getOut_time() {
        return out_time;
    }

    public void setOut_time(long out_time) {
        this.out_time = out_time;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public static DelayEvent fromString(String value){
        String[] strs = value.split(",");
        long in_time = Long.parseLong(strs[0]);
        long out_time = Long.parseLong(strs[1]);
        String val = strs[2];
        int count = Integer.parseInt(strs[3]);

        return new DelayEvent(val, in_time, out_time, count);
    }

    @Override
    public String toString() {
        return  in_time + "," +
                out_time + "," +
                value + "," +
                count;
    }
}
