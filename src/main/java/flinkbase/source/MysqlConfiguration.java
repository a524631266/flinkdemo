package flinkbase.source;

public class MysqlConfiguration {
    public static int port = 3306;
    public static String host = "192.168.10.51";
    public static String username = "observer";
    public static String password = "123456";
    public static String URL = "jdbc:mysql://"+ host + ":" + port + "/efmapi";
    public static String[] ConnectionDatabaseList = new String[]{"efmapi"};
}
