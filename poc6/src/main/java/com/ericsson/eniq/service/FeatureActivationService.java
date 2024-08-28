package com.ericsson.eniq.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ericsson.eniq.InstallerApplication.DatabaseDetails;
import com.ericsson.eniq.Services.GetDatabaseDetails;
import com.ericsson.eniq.bean.FeatureActivationRequest;
import com.ericsson.eniq.model.FeatureActivation;

import ssc.rockfactory.RockFactory;

@Service
public class FeatureActivationService {

	@Autowired
	private GetDatabaseDetails getDatabaseDetails;

	private transient RockFactory dwhrepRockFactory = null;

	private static final Logger logger = LogManager.getLogger(DatabaseDetails.class);

	public FeatureActivation getFeatureActivation(String featureDescription) {

		FeatureActivation featureActivation = new FeatureActivation();
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			final Map<String, String> databaseConnectionDetails = getDatabaseDetails.getDatabaseConnectionDetails();
			this.dwhrepRockFactory = getDatabaseDetails.createDwhrepRockFactory(databaseConnectionDetails);

			if (featureDescription != null) {
				// connection got from database
				connection = dwhrepRockFactory.getConnection();
				statement = connection.createStatement();
				// statement.execute("select * from feature_description");
				resultSet = statement.executeQuery(
						"select * from feature_activation where feature_desc='" + featureDescription + "'");

				while (resultSet.next()) {
					// Display values
					System.out.print("Feature ID: " + resultSet.getInt("f_id"));
					System.out.print(",License No : " + resultSet.getInt("license_no"));
					System.out.print(",Feature Description: " + resultSet.getString("feature_desc"));
					System.out.println(",Status: " + resultSet.getString("status"));

					featureActivation.setF_id(resultSet.getLong("f_id"));
					featureActivation.setLicenseno(resultSet.getInt("license_no"));
					featureActivation.setFeatureDescription(resultSet.getString("feature_desc"));
					featureActivation.setStatus(resultSet.getString("status"));
					featureActivation.setCreatedOn(resultSet.getTimestamp("created_on"));
					featureActivation.setUpdatedOn(resultSet.getTimestamp("updated_on"));
					featureActivation.setTechpacks(resultSet.getString("techpacks"));

				}
			}

		} catch (SQLException sqlEx) {
			logger.info("Feature Description while featching error" + sqlEx.getMessage());
		} catch (Exception ex) {
			logger.info("Feature Description while featching error" + ex.getMessage());
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return featureActivation;
	}

	public FeatureActivation updateFeatureActivation(FeatureActivationRequest featureActRequest) {
		FeatureActivation featureActivation = new FeatureActivation();
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			final Map<String, String> databaseConnectionDetails = getDatabaseDetails.getDatabaseConnectionDetails();
			this.dwhrepRockFactory = getDatabaseDetails.createDwhrepRockFactory(databaseConnectionDetails);
			// Timestamp timestamp = new Timestamp(System.currentTimeMillis());

			String updateQuery = "update feature_activation set status='" + featureActRequest.getStatus()
					+ "' , updated_on = current_timestamp " + "where feature_desc='"
					+ featureActRequest.getFeatureDesc() + "'";

			if (featureActRequest.getFeatureDesc() != null) {
				// connection got from database
				connection = dwhrepRockFactory.getConnection();
				statement = connection.createStatement();
				// statement.execute("select * from feature_description");
				int update = statement.executeUpdate(updateQuery);

				if (update == 1) {
					resultSet = statement.executeQuery("select * from feature_activation where feature_desc='"
							+ featureActRequest.getFeatureDesc() + "'");
					while (resultSet.next()) {
						// Display values
						System.out.print("Feature ID: " + resultSet.getInt("f_id"));
						System.out.print(",License No : " + resultSet.getInt("license_no"));
						System.out.print(",Feature Description: " + resultSet.getString("feature_desc"));
						System.out.println(",Status: " + resultSet.getString("status"));
						System.out.println(",Created On: " + resultSet.getString("created_on"));
						System.out.println(",Updated On: " + resultSet.getString("updated_on"));
						System.out.println(", Techpacks: " + resultSet.getString("techpacks"));

						featureActivation.setF_id(resultSet.getLong("f_id"));
						featureActivation.setLicenseno(resultSet.getInt("license_no"));
						featureActivation.setFeatureDescription(resultSet.getString("feature_desc"));
						featureActivation.setStatus(resultSet.getString("status"));
						featureActivation.setCreatedOn(resultSet.getTimestamp("created_on"));
						featureActivation.setUpdatedOn(resultSet.getTimestamp("updated_on"));
						featureActivation.setTechpacks(resultSet.getString("techpacks"));

					}
				}
				/*
				 * UPDATE config SET t1.config_value = 'value' , t2.config_value = 'value2'
				 * WHERE t1.config_name = 'name1' AND t2.config_name = 'name2';
				 */

			}

		} catch (SQLException sqlEx) {
			logger.info("Feature Description while featching error" + sqlEx.getMessage());
			sqlEx.printStackTrace();
		} catch (Exception ex) {
			logger.info("Feature Description while featching error" + ex.getMessage());
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return featureActivation;
	}

	public FeatureActivation intallFeature(FeatureActivationRequest activationRequest) throws Exception {

		FeatureActivation featureActivation = new FeatureActivation();
		Connection connection = null;
		Statement statement = null;
		PreparedStatement prepStament = null;
		ResultSet resultSet = null;

		try {

			final Map<String, String> databaseConnectionDetails = getDatabaseDetails.getDatabaseConnectionDetails();
			this.dwhrepRockFactory = getDatabaseDetails.createDwhrepRockFactory(databaseConnectionDetails);

			// Connection retrieving from dwhrep database
			connection = dwhrepRockFactory.getConnection();
			prepStament = connection.prepareStatement(
					"insert into feature_activation(f_id, license_no, feature_desc, techpacks, status, created_on, updated_on) "
							+ "values(?,?,?,?,?,current_timestamp, current_timestamp)");

			prepStament.setLong(1, activationRequest.getfId());
			prepStament.setInt(2, activationRequest.getLicenseNo());
			prepStament.setString(3, activationRequest.getFeatureDesc());
			prepStament.setString(4, activationRequest.getTechpacks());
			prepStament.setString(5, activationRequest.getStatus());
			prepStament.execute();

			statement = connection.createStatement();
			resultSet = statement.executeQuery(
					"select * from feature_activation where feature_desc='" + activationRequest.getFeatureDesc() + "'");
			while (resultSet.next()) {
				// Display values
				System.out.print("Feature ID: " + resultSet.getInt("f_id"));
				System.out.print(",License No : " + resultSet.getInt("license_no"));
				System.out.print(",Feature Description: " + resultSet.getString("feature_desc"));
				System.out.println(",Status: " + resultSet.getString("status"));

				featureActivation.setF_id(resultSet.getLong("f_id"));
				featureActivation.setLicenseno(resultSet.getInt("license_no"));
				featureActivation.setFeatureDescription(resultSet.getString("feature_desc"));
				featureActivation.setStatus(resultSet.getString("status"));
				featureActivation.setCreatedOn(resultSet.getTimestamp("created_on"));
				featureActivation.setUpdatedOn(resultSet.getTimestamp("updated_on"));
				featureActivation.setTechpacks(resultSet.getString("techpacks"));

			}

		} catch (SQLException sqlEx) {
			logger.info("while executing query getting error - " + sqlEx.getMessage());
			throw sqlEx;
		} catch (Exception ex) {
			logger.info("while install feature getting error - " + ex.getMessage());
			throw ex;
		} finally {
			try {
				if (resultSet != null)
					resultSet.close();
			} catch (SQLException e) {
				logger.info("while resultSet closing error - " + e.getMessage());
			}
			try {
				if (prepStament != null)
					prepStament.close();
			} catch (SQLException e) {
				logger.info("while statement closing error - " + e.getMessage());
			}
			try {
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				logger.info("while connection closing error - " + e.getMessage());
			}

		}

		return featureActivation;
	}

}
