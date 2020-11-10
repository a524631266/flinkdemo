package flinkbase.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Poi{
    String addr;
    String cp;
    String direction;
    String distance;
    String name;
    String poiType;
    Point point;

    String tag;
    String tel;
    String uid;
    String zip;
    ParentPoi parent_poi;
}