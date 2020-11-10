package flinkbase.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@NoArgsConstructor
public class AddressComponent{
    String country;
    int country_code;
    String country_code_iso;
    String country_code_iso2;
    String province;
    String city;
    int city_level;
    String district;
    String town;
    String town_code;
    String adcode;
    String street;
    String street_number;
    String direction;
    String distance;
}