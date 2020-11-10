package flinkbase.transformation;

import flinkbase.model.CityMap;
import flinkbase.model.GeoCityMapper;
import flinkbase.model.Location;
import org.junit.Test;

import java.net.MalformedURLException;

import static org.junit.Assert.*;

public class BaiduMapServiceTest {

    @Test
    public void getGeocoderLatitude() {
        GeoCityMapper shMapper = new BaiduMapService().getGeocoderLatitude("上海");
        System.out.println(shMapper);
        Location location = shMapper.getResult().getLocation();
        double lat = location.getLat();
        assertEquals(31.235929042252014, lat);
        double lng = location.getLng();
        assertEquals(121.48053886017651, lng);
    }


    @Test
    public void getposition() throws MalformedURLException {
//        CityMap getposition = new BaiduMapService().getposition("31.325152", "120.558957");
        CityMap getposition = new BaiduMapService().getposition("31.2359290422", "121.4805388");
        System.out.println(getposition);
    }
}