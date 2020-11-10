package flinkbase.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;


@Data
@NoArgsConstructor
@ToString
public class GeoCityResult {
    Location location;
    int precise;
    int confidence;
    int comprehension;
    String level;
}
