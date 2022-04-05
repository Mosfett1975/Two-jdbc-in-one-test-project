/**
 * NOTE: It's just a demo. Yes, something was not written correctly, but at least it worked :)
 */



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class JdbcDemo {

    String CLICK_HOST = "click.local";
    String CLICK_NAME = "test";
    String CLICK_USER = "dev";
    String CLICK_PASS = "dev";
    String CLICK_URL = String.format( "jdbc:clickhouse://%s:8123/%s", CLICK_HOST, CLICK_NAME );

    String POSTGRES_HOST = "pgsql.local";
    String POSTGRES_NAME = "target";
    String POSTGRES_USER = "dev";
    String POSTGRES_PASS = "dev";
    String POSTGRES_URL = String.format( "jdbc:postgresql://%s:5432/%s", POSTGRES_HOST, POSTGRES_NAME );


    public void getTimeClick () {
        createSqlRequest ("SELECT now()", "Click");
    }

    public void getTimePost () {
        createSqlRequest ("SELECT now()", "Post");
    }

    public String createSqlRequest(String request, String baseType){
        String value = new String();
        String DB_URL = new String();
        String DB_USER = new String();
        String DB_PASS = new String();
        String name = new String();
        if (baseType.equals("Click")){
            DB_URL=CLICK_URL;
            DB_USER=CLICK_USER;
            DB_PASS=CLICK_PASS;
            name = "ru.yandex.clickhouse.ClickHouseDriver";
        }
        if (baseType.equals("Post")){
            DB_URL=POSTGRES_URL;
            DB_USER=POSTGRES_USER;
            DB_PASS=POSTGRES_PASS;
            name = "org.postgresql.Driver";
        }

        try {
            Class.forName( name );
            Connection conn = DriverManager.getConnection( DB_URL, DB_USER, DB_PASS );
            // Condition with DELETE is necessary so that there is no exception as DELETE will return NULL
            if (request.contains("DELETE")){ int rs = conn.createStatement().executeUpdate( request );}
            else{
                ResultSet rs = conn.createStatement().executeQuery( request );
                if ( rs != null && rs.next() ) {
                    value = rs.getString( 1 );
                }}
            conn.close();
        } catch ( Exception ex ) {
            ex.printStackTrace();
        }

        return value;

    }
}
