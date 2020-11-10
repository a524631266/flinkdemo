package flinkbase.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@NoArgsConstructor
public class ParentPoi{
    String name;
    String tag;
    String addr;
    String direction;
    String distance;
    String uid;
    Point point;
}
