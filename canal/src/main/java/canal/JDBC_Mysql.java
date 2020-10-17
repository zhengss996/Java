package canal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBC_Mysql {

    public static void insert(String sql) {
        try {
            Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
            String url="jdbc:mysql://localhost:3306/canal_ods";//声明数据库test的url
            String user="root";//数据库的用户名
            String password="root";//数据库的密码
            //建立数据库连接，获得连接对象conn(抛出异常即可)
            Connection conn= DriverManager.getConnection(url, user, password);
            System.out.println("连接数据库成功");
            //生成一条mysql语句
            System.out.println(sql);
            Statement stmt=conn.createStatement();//创建一个Statement对象
            stmt.executeUpdate(sql);//执行sql语句
            System.out.println("插入到数据库成功");
            conn.close();
            System.out.println("关闭数据库成功");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
