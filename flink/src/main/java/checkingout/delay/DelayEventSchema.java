package checkingout.delay;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created by Zhao Qing on 2018/5/24.
 */
public class DelayEventSchema implements DeserializationSchema<DelayEvent>, SerializationSchema<DelayEvent> {
    private static final long serialVersionUID = 6154188370181669768L;

    @Override
    public byte[] serialize(DelayEvent event){
        return event.toString().getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public DelayEvent deserialize(byte[] message) throws IOException {
        return DelayEvent.fromString(new String (message, Charset.forName("UTF-8")));
    }

    @Override
    public boolean isEndOfStream(DelayEvent nextElement){
        return false;
    }

    @Override
    public TypeInformation<DelayEvent> getProducedType(){
        return TypeInformation.of(DelayEvent.class);
    }
}
