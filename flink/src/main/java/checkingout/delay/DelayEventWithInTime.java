package checkingout.delay;

/**
 * Created by Zhao Qing on 2018/5/24.
 */
public class DelayEventWithInTime {
    private long in_time;

    private String value;

    private int count;

    public DelayEventWithInTime(){}

    public DelayEventWithInTime(long in_time, String value, int count) {
        this.in_time = in_time;
        this.value = value;
        this.count = count;
    }

    public long getIn_time() {
        return in_time;
    }

    public void setIn_time(long in_time) {
        this.in_time = in_time;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return  in_time + "," +
                value + "," +
                count;
    }
}
