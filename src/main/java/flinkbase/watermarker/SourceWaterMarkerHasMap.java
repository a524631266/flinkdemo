package flinkbase.watermarker;

import flinkbase.model.Person;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * 在source端，生成数据，并发送数据
 */
public class SourceWaterMarkerHasMap {
    /**
     *
     * 滑动窗口 设置
     * source:  1546444800000
     * Person(age=2, name=王五, address=Address(id=774, address=日害堆), birthDay=2019-01-03)
     * source:  1546790400000
     * Person(age=6, name=王五, address=Address(id=300, address=弊群假), birthDay=2019-01-07)
     * 5> Person(age=8, name=王五, address=Address(id=774, address=日害堆), birthDay=2019-01-03)
     * source:  1547136000000
     * Person(age=10, name=王五, address=Address(id=924, address=摊), birthDay=2019-01-11)
     * source:  1547481600000
     * Person(age=14, name=王五, address=Address(id=255, address=挑轿编), birthDay=2019-01-15)
     * 5> Person(age=32, name=王五, address=Address(id=774, address=日害堆), birthDay=2019-01-03)
     * source:  1547827200000
     * Person(age=18, name=王五, address=Address(id=107, address=插), birthDay=2019-01-19)
     * source:  1548172800000
     * Person(age=22, name=王五, address=Address(id=272, address=马), birthDay=2019-01-23)
     * 5> Person(age=70, name=王五, address=Address(id=300, address=弊群假), birthDay=2019-01-07)
     * source:  1548518400000
     * Person(age=26, name=王五, address=Address(id=586, address=凉路招), birthDay=2019-01-27)
     * source:  1548864000000
     * Person(age=30, name=王五, address=Address(id=735, address=职概), birthDay=2019-01-31)
     * 5> Person(age=110, name=王五, address=Address(id=255, address=挑轿编), birthDay=2019-01-15)
     * source:  1549209600000
     * Person(age=34, name=王五, address=Address(id=857, address=汗糕漫), birthDay=2019-02-04)
     * source:  1549555200000
     * Person(age=38, name=王五, address=Address(id=803, address=问再莲), birthDay=2019-02-08)
     * 5> Person(age=150, name=王五, address=Address(id=272, address=马), birthDay=2019-01-23)
     * source:  1549900800000
     * Person(age=42, name=王五, address=Address(id=312, address=告纳阅), birthDay=2019-02-12)
     * source:  1550246400000
     * Person(age=46, name=王五, address=Address(id=510, address=骡干), birthDay=2019-02-16)
     * 5> Person(age=190, name=王五, address=Address(id=735, address=职概), birthDay=2019-01-31)
     * source:  1550592000000
     * Person(age=50, name=王五, address=Address(id=780, address=胳善), birthDay=2019-02-20)
     * source:  1550937600000
     * Person(age=54, name=王五, address=Address(id=247, address=作芒), birthDay=2019-02-24)
     * 5> Person(age=230, name=王五, address=Address(id=803, address=问再莲), birthDay=2019-02-08)
     * source:  1551283200000
     * Person(age=58, name=王五, address=Address(id=439, address=赢三), birthDay=2019-02-28)
     * source:  1551628800000
     * Person(age=62, name=王五, address=Address(id=610, address=长), birthDay=2019-03-04)
     * 5> Person(age=270, name=王五, address=Address(id=510, address=骡干), birthDay=2019-02-16)
     * source:  1551974400000
     * Person(age=66, name=王五, address=Address(id=241, address=萍肃截), birthDay=2019-03-08)
     * source:  1552320000000
     * Person(age=70, name=王五, address=Address(id=703, address=宾凶), birthDay=2019-03-12)
     * 5> Person(age=310, name=王五, address=Address(id=247, address=作芒), birthDay=2019-02-24)
     * source:  1552665600000
     * Person(age=74, name=王五, address=Address(id=888, address=丹), birthDay=2019-03-16)
     * source:  1553011200000
     * Person(age=78, name=王五, address=Address(id=496, address=式), birthDay=2019-03-20)
     * 5> Person(age=350, name=王五, address=Address(id=610, address=长), birthDay=2019-03-04)
     * source:  1553356800000
     * Person(age=82, name=王五, address=Address(id=952, address=荐), birthDay=2019-03-24)
     * source:  1553702400000
     * Person(age=86, name=王五, address=Address(id=975, address=纠沾办), birthDay=2019-03-28)
     * 5> Person(age=390, name=王五, address=Address(id=703, address=宾凶), birthDay=2019-03-12)
     * source:  1554048000000
     * Person(age=90, name=王五, address=Address(id=404, address=删), birthDay=2019-04-01)
     * source:  1554393600000
     * Person(age=94, name=王五, address=Address(id=926, address=据链歇), birthDay=2019-04-05)
     * 5> Person(age=430, name=王五, address=Address(id=496, address=式), birthDay=2019-03-20)
     * source:  1554739200000
     * Person(age=98, name=王五, address=Address(id=259, address=暴教), birthDay=2019-04-09)
     * source:  1555084800000
     * Person(age=1, name=王五, address=Address(id=708, address=俩狂), birthDay=2019-04-13)
     * 5> Person(age=369, name=王五, address=Address(id=975, address=纠沾办), birthDay=2019-03-28)
     * source:  1555430400000
     * Person(age=5, name=王五, address=Address(id=217, address=束够), birthDay=2019-04-17)
     * source:  1555776000000
     * Person(age=9, name=王五, address=Address(id=823, address=夏敞), birthDay=2019-04-21)
     * 5> Person(age=207, name=王五, address=Address(id=926, address=据链歇), birthDay=2019-04-05)
     * source:  1556121600000
     * Person(age=13, name=王五, address=Address(id=81, address=妥), birthDay=2019-04-25)
     * source:  1556467200000
     * Person(age=17, name=王五, address=Address(id=232, address=缝), birthDay=2019-04-29)
     * 5> Person(age=45, name=王五, address=Address(id=708, address=俩狂), birthDay=2019-04-13)
     * source:  1556812800000
     * Person(age=21, name=王五, address=Address(id=691, address=腥), birthDay=2019-05-03)
     * source:  1557158400000
     * Person(age=25, name=王五, address=Address(id=381, address=导种), birthDay=2019-05-07)
     * 5> Person(age=85, name=王五, address=Address(id=823, address=夏敞), birthDay=2019-04-21)
     * source:  1557504000000
     * Person(age=29, name=王五, address=Address(id=650, address=伙), birthDay=2019-05-11)
     * source:  1557849600000
     * Person(age=33, name=王五, address=Address(id=810, address=摇), birthDay=2019-05-15)
     * 5> Person(age=125, name=王五, address=Address(id=232, address=缝), birthDay=2019-04-29)
     * source:  1558195200000
     * Person(age=37, name=王五, address=Address(id=854, address=臂絮脂), birthDay=2019-05-19)
     * source:  1558540800000
     * Person(age=41, name=王五, address=Address(id=681, address=恶龟靠), birthDay=2019-05-23)
     * 5> Person(age=165, name=王五, address=Address(id=381, address=导种), birthDay=2019-05-07)
     * source:  1558886400000
     * Person(age=45, name=王五, address=Address(id=576, address=审画患), birthDay=2019-05-27)
     * source:  1559232000000
     * Person(age=49, name=王五, address=Address(id=694, address=沾崇), birthDay=2019-05-31)
     * 5> Person(age=205, name=王五, address=Address(id=810, address=摇), birthDay=2019-05-15)
     * source:  1559577600000
     * Person(age=53, name=王五, address=Address(id=923, address=票), birthDay=2019-06-04)
     * source:  1559923200000
     * Person(age=57, name=王五, address=Address(id=739, address=征), birthDay=2019-06-08)
     * 5> Person(age=245, name=王五, address=Address(id=681, address=恶龟靠), birthDay=2019-05-23)
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        env.setParallelism(10);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 1. 在source端 发送水印时间
        SourceFunction<Person> source = SourceUtil.
                createStreamSourceWithWatherMark(Person.class ,"birthDay");

        SingleOutputStreamOperator<Person> source1 = env.addSource(source)
                .returns(Person.class);

        source1
                .filter(person -> "王五".equals(person.getName()))
                .keyBy(person-> person.getName())
//                .timeWindowAll(Time.days(2))
                .countWindow(5, 2)
                .sum("age").printToErr()
        ;

        env.execute("water marker1");

    }
}
