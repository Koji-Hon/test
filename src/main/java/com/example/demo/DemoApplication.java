package com.example.demo;

import java.sql.DriverManager;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@SpringBootApplication
@Configuration
public class DemoApplication {

	public static void main(String[] args) {
		String[] test = new String[4];
		test[0] = "jdbc:vdb://localhost:9999/";
		test[1] = "admin";
		test[2] = "admin";
		test[3] = "insert into viewer (view1,view2,view3,view4) values('v11','v12','v13','v14')";

		// Check parameters number (uri, user, password)
		if (test.length != 4) {

			System.out.println("Invalid parameters number");
			System.out.println("Usage: java com.denodo.vdp.demo.jdbcclient.JDBCClient "
					+ "<uri> <login> <password> <sql-like-query>");
			System.out.println("Example: java com.denodo.vdp.demo.jdbcclient.JDBCClient "
					+ "jdbc:vdb://localhost:9999/admin admin admin \"select * from view\"");
			System.exit(-1);
		}

		System.out.println("Connecting to database ... '" + test[0] + "'");
		System.out.println("User: '" + test[1] + "'");
		System.out.println("Password: '" + test[2] + "'");
		System.out.println("SQL-like-QUERY: '" + test[3] + "'");

		// JDBC Driver load
		try {

			Class.forName("com.denodo.vdp.jdbc.Driver");

		} catch (Exception e) {

			System.err.println("Error loading VDPDriver ... " + e.getMessage());
			System.exit(-1);
		}

		// Connect the driver to the database
		Connection connection = null;
		try {

			connection = DriverManager.getConnection(test[0], test[1], test[2]);

			// Create the statement from the user query
			PreparedStatement statement = connection.prepareStatement(test[3]);

			// Execute the query
			statement.execute();

			// Obtaining the result of the query and printing it.
			ResultSet result = statement.getResultSet();
			if (result != null) {
				ResultSetMetaData metadata = result.getMetaData();
				int max = metadata.getColumnCount();

				System.out.print("| ");
				for (int i = 1; i <= max; i++) {

					System.out.print(metadata.getColumnName(i) + ":" + metadata.getColumnTypeName(i) + " | ");
				}
				System.out.println("\n");

				// Iterate over the rows returned
				while (result.next()) {

					System.out.print("| ");
					// Iterate over the columns of the row
					for (int i = 1; i <= max; i++) {

						System.out.print(result.getObject(i) + " | ");
					}
					System.out.println();
				}
			} else {

				System.err.println("Error in result");
			}

		} catch (SQLException e) {

			System.out.println("Error connecting the database: " + e.getMessage());
		} finally {

			// The connection must be closed.
			if (connection != null) {

				try {

					connection.close();

				} catch (SQLException e) {

					e.printStackTrace();
				}
			}
		}

	}
}
