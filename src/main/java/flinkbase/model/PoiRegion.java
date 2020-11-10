package flinkbase.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@NoArgsConstructor
public class PoiRegion{
    String direction_desc;
    String name;
    String tag;
    String uid;
    int distance;
}