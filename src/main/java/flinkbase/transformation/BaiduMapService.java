package flinkbase.transformation;

import flinkbase.model.CityMap;
import flinkbase.model.GeoCityMapper;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.type.TypeReference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BaiduMapService {

    private static Pattern compile = Pattern.compile("(\\{.*\\})");
    private static ObjectMapper mapper;
    static {
        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonSerialize.Inclusion.ALWAYS);
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS , false);
    }
    public GeoCityMapper getGeocoderLatitude(String address) {
        BufferedReader in = null;
        try {
            address = URLEncoder.encode(address, "UTF-8");
            URL tirc = new URL("http://api.map.baidu.com/geocoder/v2/?address=" + address + "&output=json&ak="
                    + "rt7sBgcP6i3l8x9IimiFCDi7XVa1Sjkx");
            in = new BufferedReader(new InputStreamReader(tirc.openStream(), "UTF-8"));
            String res;
            StringBuilder sb = new StringBuilder("");
            while ((res = in.readLine()) != null) {
                sb.append(res.trim());
            }
            String result = sb.toString();
            if (StringUtils.isNotEmpty(result)) {
                GeoCityMapper object = mapper.readValue(result, new TypeReference<GeoCityMapper>() {});
                return object;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }


    public CityMap getposition(String latitude, String longitude) throws MalformedURLException {
        BufferedReader in = null;
        URL tirc = new URL("http://api.map.baidu.com/geocoder/v2/?callback=renderReverse&location=" + latitude + "," + longitude
                + "&output=json&pois=1&ak=" + "rt7sBgcP6i3l8x9IimiFCDi7XVa1Sjkx");
        String result = null;
        try {
            in = new BufferedReader(new InputStreamReader(tirc.openStream(), "UTF-8"));
            String res;
            StringBuilder sb = new StringBuilder("");
            while ((res = in.readLine()) != null) {
                sb.append(res.trim());
            }
            result = sb.toString();
            if (StringUtils.isNotEmpty(result)) {

                Matcher matcher = compile.matcher(result);
                if (matcher.find()) {
                    String group = matcher.group(1);
                    // wrong不能转义
                    // CityMap cityMap = mapper.convertValue(group, new TypeReference<CityMap>(){});
                    CityMap cityMap = mapper.readValue(group, new TypeReference<CityMap>(){});
                    return cityMap;
                }

            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
