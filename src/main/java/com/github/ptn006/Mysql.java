package com.github.ptn006;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Mysql {

    public static Connection con() throws SQLException {

        Connection myCon = DriverManager.getConnection("jdbc:mysql://localhost:3306/sys",
                "root", "root");

        return myCon;
    }
}
