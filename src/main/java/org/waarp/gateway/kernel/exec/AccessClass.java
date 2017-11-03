package org.waarp.gateway.kernel.exec;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AccessClass {
	private static String folderName;
	private static String SpecialKey;
	// private static String FileId;
	private static String ParentId;
	private static long specialId;
	private static String key;
	private static String bucketName;
	private static String fileId;

	public void insert(String fileName, long specialKey, String s3url) throws SQLException {

		String Key = key;
		String url = "jdbc:mysql://localhost:3306/firstwaarp";
		String user = "root";
		String password = "root";
		try {
			Connection conn = (Connection) DriverManager.getConnection(url, user, password);
			String query = " insert into sftp_storagebucketmapping ( specialKey, fileName, s3fileurl)"
					+ " values (?, ?, ?)";

			PreparedStatement preparedStmt = conn.prepareStatement(query);

			
			preparedStmt.setLong(1, specialKey);
			preparedStmt.setString(2, fileName);
			preparedStmt.setString(3, s3url);
			

			preparedStmt.execute();

			conn.close();
		} 
			
		catch (SQLException e) {
			e.printStackTrace();
		}

	}	
		public String TakeSpecialId(String file)
		{
			String splId=null;
			String url1 = "jdbc:mysql://localhost:3306/firstwaarp";
			String user1 = "root";
			String password1 = "root";
			try {
				Connection conn = (Connection) DriverManager.getConnection(url1, user1, password1);
				String query = " select specialKey from sftp_storagebucketmapping where fileName =?";

				PreparedStatement preparedStmt = conn.prepareStatement(query);

				preparedStmt.setNString(1, file);
				
				ResultSet rs = preparedStmt.executeQuery();
				rs.next();
			    splId=rs.getString(1);
			
				conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
			return splId;	
		}
		public String getOriginalname(String file)
		{
			String path = null;
			String url1 = "jdbc:mysql://localhost:3306/firstwaarp";
			String user1 = "root";
			String password1 = "root";
			try {
				Connection conn = (Connection) DriverManager.getConnection(url1, user1, password1);
				String query = " select ORIGINALNAME from runner where FILENAME =?";

				PreparedStatement preparedStmt = conn.prepareStatement(query);

				preparedStmt.setNString(1, file);
				
				ResultSet rs = preparedStmt.executeQuery();
				rs.next();
			    path=rs.getString(1);
			
				conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
			return path;	

		}
		
	}


