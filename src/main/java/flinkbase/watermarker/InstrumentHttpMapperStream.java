package flinkbase.watermarker;

import flinkbase.model.CallCity;
import flinkbase.model.CityMap;
import flinkbase.model.GeoCityMapper;
import flinkbase.transformation.BaiduMapService;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.SourceUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 根据baiduService创建一个请求流
 */
public class InstrumentHttpMapperStream {
    public static BaiduMapService baiduMapService = new BaiduMapService();

    /**
     * 3> GeoCityMapper(status=0, result=GeoCityResult(location=Location(lng=113.27143134445974, lat=23.135336306695006), precise=0, confidence=20, comprehension=100, level=城市))
     * CallCity(cityName=杭州西湖)
     * hhhhhhhh
     * 4> GeoCityMapper(status=0, result=GeoCityResult(location=Location(lng=120.13643801205312, lat=30.265916325588922), precise=0, confidence=20, comprehension=100, level=区县))
     * CallCity(cityName=上海)
     * hhhhhhhh
     * 1> GeoCityMapper(status=0, result=GeoCityResult(location=Location(lng=121.48053886017651, lat=31.235929042252014), precise=0, confidence=20, comprehension=100, level=城市))
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
//        StreamExecutionEnvironment env = EnvUtil.createDefaultRemote();
        SourceFunction<CallCity> streamSource = SourceUtil.createStreamSource(CallCity.class);

        SingleOutputStreamOperator<CallCity> returns = env.addSource(streamSource).returns(CallCity.class);

        method1(returns);
//        mehod2(returns);

        env.execute("asdf");
    }

    private static void mehod2(SingleOutputStreamOperator<CallCity> returns) throws Exception {
        returns.map(new RichMapFunction<CallCity, GeoCityMapper>() {
            public BaiduMapService baiduMapService = new BaiduMapService();
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
//                getRuntimeContext().getS
            }

            @Override
            public GeoCityMapper map(CallCity value) throws Exception {
                System.out.println(baiduMapService);
                if(baiduMapService != null){
                    GeoCityMapper geocoderLatitude = baiduMapService.getGeocoderLatitude(value.getCityName());
                    return geocoderLatitude;
                };
                return null;
            }
        });
    }

    private static void method1(SingleOutputStreamOperator<CallCity> returns) {
        returns.map(new MapFunction<CallCity, GeoCityMapper>() {
            @Override
            public GeoCityMapper map(CallCity value) throws Exception {
//                System.out.println("hhhhhhhh ");
                System.out.println(baiduMapService);
                GeoCityMapper geocoderLatitude = baiduMapService.getGeocoderLatitude(value.getCityName());
                return geocoderLatitude;
            }
        }).printToErr();
    }
}
