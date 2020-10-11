package flinkbase.hdfs;

import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PersonHD implements WritableComparable<PersonHD> {
    private Text name = new Text();
    private IntWritable age = new IntWritable();
    private Text sex = new Text();

    public PersonHD(String name, int age, String sex) {
        this.name.set(name);
        this.age.set(age);
        this.sex.set(sex);
    }

    public PersonHD() {
    }


    @VisibleForTesting
    protected void setNull(){
        name = null;
        age = null;
        sex = null;
    }


    public void set(String name, int age, String sex) {
        this.name.set(name);
        this.age.set(age);
        this.sex.set(sex);
    }

    @Override
    public int compareTo(PersonHD o) {
        if(o ==null){
            throw new RuntimeException("PersonHD is null");
        }
        if(this.name == null && o.name != null){
            return -1;
        }
        if(this.age == null && o.age != null){
            return -1;
        }
        if(this.sex == null && o.sex != null){
            return -1;
        }
        int i = this.name.compareTo(o.name);
        if(i != 0){
            return i;
        }
        i = this.age.compareTo(o.age);
        if(i != 0){
            return i;
        }
        return this.sex.compareTo(o.sex);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        name.write(dataOutput);
        this.age.write(dataOutput);
        sex.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name.readFields(dataInput);
        age.readFields(dataInput);
        sex.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        final int left = 5;
        int base = 1;
        base = (base << left) + ((name == null) ?0: name.hashCode());
        base = (base << left) + ((age == null) ?0: age.hashCode());
        base = (base << left) + ((sex == null) ?0: sex.hashCode());
//        base = base * step + ((name == null) ?0: name.hashCode());
//        base = base * step + ((age == null) ?0: age.hashCode());
//        base = base * step + ((sex == null) ?0: sex.hashCode());
        return base;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null){
            return false;
        }
        if(this == obj){
            return true;
        }
        if(obj.getClass() != this.getClass()){
            return false;
        }
        if(!(obj instanceof PersonHD)){
            return false;
        }else {

            int i = this.compareTo((PersonHD) obj);

            if(i != 0 ){
                return false;
            }else {
                return true;
            }

        }
    }
}
