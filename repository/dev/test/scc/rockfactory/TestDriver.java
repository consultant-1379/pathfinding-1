package scc.rockfactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class TestDriver implements Driver {

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (info.getProperty("user").equalsIgnoreCase("failed_login")) {
      //replicate a Sybase IQ login error
      final SQLException cause = new SQLException("SQL Anywhere Error -103: Invalid user ID or password", "01000", 4002);
      final SQLException throwing = new SQLException("JZ00L: Login failed.  Examine the SQLWarnings chained to this " +
        "exception for the reason(s).", "JZ00L");
      throwing.setNextException(cause);
      throw throwing;
    }
    return null;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return false;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }


public Logger getParentLogger() throws SQLFeatureNotSupportedException {
	// TODO Auto-generated method stub
	return null;
}
}
