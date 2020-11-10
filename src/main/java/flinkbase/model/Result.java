package flinkbase.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.ArrayList;

@Data
@NoArgsConstructor
public class Result{
    Location location;
    String formatted_address;
    String business;
    AddressComponent addressComponent;
    String sematic_description;
    int cityCode;
    ArrayList<Poi> pois;
    ArrayList<PoiRegion> poiRegions;
    ArrayList<Road> roads;
}
