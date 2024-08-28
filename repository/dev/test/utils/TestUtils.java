package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import ssc.rockfactory.RockFactory;


public class TestUtils {
	private static boolean isFirstTime = true ;
  private final static String _createStatementMetaFile = "TableCreateStatements.sql";
  private final static String _viewStatementMetaFile = "ViewCreateStatements.sql";

  public static void loadSetup(RockFactory rock, String base) throws Exception{
		final String lookFor = "setupSQL/"+base;
    final URL url = ClassLoader.getSystemResource(lookFor);
		if(url == null){
			throw new Exception("Couldn't find '"+lookFor+"' on classpath"); 
		}
    final String baseDir = url.toURI().getRawPath();
    final File loadFrom = new File(baseDir);
    final File[] toLoad = loadFrom.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(".sql") && !name.equals(_createStatementMetaFile)
        &&!name.equals(_viewStatementMetaFile);
      }
    });
    final Statement stmt = rock.getConnection().createStatement();
    try{
    	if(isFirstTime){
    		stmt.executeUpdate("create schema dc AUTHORIZATION DBA; create schema dcpublic AUTHORIZATION DBA;");
    		isFirstTime = false;
    	}
		} finally {
      stmt.close();
    }
    final File createFile = new File(baseDir + "/" + _createStatementMetaFile);
    loadSqlFile(createFile, rock);
    for (File loadFile : toLoad) {
      loadSqlFile(loadFile, rock);
    }
  }

  public static void loadSetup(Connection connection, String base) throws Exception{
		final String lookFor = "setupSQL/"+base;
  final URL url = ClassLoader.getSystemResource(lookFor);
		if(url == null){
			throw new Exception("Couldn't find '"+lookFor+"' on classpath"); 
		}
  final String baseDir = url.toURI().getRawPath();
  final File loadFrom = new File(baseDir);
  final File[] toLoad = loadFrom.listFiles(new FilenameFilter() {
    public boolean accept(File dir, String name) {
      return name.endsWith(".sql") && !name.equals(_createStatementMetaFile)
      &&!name.equals(_viewStatementMetaFile);
    }
  });
  final Statement stmt = connection.createStatement();
  try{
    stmt.executeUpdate("create schema dc AUTHORIZATION DBA; create schema dcpublic AUTHORIZATION DBA;");
		} finally {
    stmt.close();
  }
  final File createFile = new File(baseDir + "/" + _createStatementMetaFile);
  loadSqlFile(createFile, connection);
  for (File loadFile : toLoad) {
    loadSqlFile(loadFile, connection);
  }
}

  private static void loadSqlFile(final File sqlFile, final Connection connection) throws IOException, SQLException, ClassNotFoundException {
	    if (!sqlFile.exists()) {
	      return;
	    }
	    BufferedReader br = new BufferedReader(new FileReader(sqlFile));
	    String line;
	    int lineCount = 0;
	    try {
	      while ((line = br.readLine()) != null) {
	        lineCount++;
	        line = line.trim();
	        if (line.length() == 0 || line.startsWith("#")) {
	          continue;
	        }
	        while (!line.endsWith(";")) {
	          final String tmp = br.readLine();
	          if (tmp != null) {
	            line += "\r\n";
	            line += tmp;
	          } else {
	            break;
	          }
	        }
	        update(line, connection);
	      }
	      connection.commit();
	    } catch (SQLException e) {
	      throw new SQLException("Error executing on line [" + lineCount + "] of " + sqlFile, e);
	    } finally {
	      br.close();
	    }
	  }

  private static void loadSqlFile(final File sqlFile, final RockFactory rock) throws IOException, SQLException, ClassNotFoundException {
    if (!sqlFile.exists()) {
      return;
    }
    BufferedReader br = new BufferedReader(new FileReader(sqlFile));
    String line;
    int lineCount = 0;
    try {
      while ((line = br.readLine()) != null) {
        lineCount++;
        line = line.trim();
        if (line.length() == 0 || line.startsWith("#")) {
          continue;
        }
        while (!line.endsWith(";")) {
          final String tmp = br.readLine();
          if (tmp != null) {
            line += "\r\n";
            line += tmp;
          } else {
            break;
          }
        }
        update(line, rock);
      }
      rock.commit();
    } catch (SQLException e) {
      throw new SQLException("Error executing on line [" + lineCount + "] of " + sqlFile, e);
    } finally {
      br.close();
    }
  }
  
  private static void update(final String insertSQL, final Connection connection) throws SQLException, ClassNotFoundException, IOException {
	    final Statement s = connection.createStatement();
	    try {
	      s.executeUpdate(insertSQL);
	    } catch (SQLException e) {
	      if (e.getSQLState().equals("S0004")) {
	        System.out.println("Views not supported yet: " + e.getMessage());
	      } else if (e.getSQLState().equals("S0001") || e.getSQLState().equals("42504")) {
	        //ignore, table already exists.......
	      } else {
	        throw e;
	      }
	    }
	  }

  private static void update(final String insertSQL, final RockFactory rock) throws SQLException, ClassNotFoundException, IOException {
    final Statement s = rock.getConnection().createStatement();
    try {
      s.executeUpdate(insertSQL);
    } catch (SQLException e) {
      if (e.getSQLState().equals("S0004")) {
        System.out.println("Views not supported yet: " + e.getMessage());
      } else if (e.getSQLState().equals("S0001") || e.getSQLState().equals("42504")) {
        //ignore, table already exists.......
      } else {
        throw e;
      }
    }
  }
    /**
   * shutdown the rockFacotry
   * @param db conn
   */
  public static void shutdown(final RockFactory db) {
    try {
      if (db != null && !db.getConnection().isClosed()) {
        final Statement stmt = db.getConnection().createStatement();
        stmt.executeUpdate("SHUTDOWN");
        stmt.close();
        db.getConnection().close();
      }
    } catch (Throwable t) {
      // ignore
    }
  }

  public static void shutdown(final Connection connection) {
	    try {
	      if (connection != null && !connection.isClosed()) {
	        final Statement stmt = connection.createStatement();
	        stmt.executeUpdate("SHUTDOWN");
	        stmt.close();
	        connection.close();
	      }
	    } catch (Throwable t) {
	      // ignore
	    }
	  }


}
