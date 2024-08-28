package ssc.rockfactory;

import java.sql.SQLException;
import java.util.Set;


public interface RockDBObject {

  Set<String> gimmeModifiedColumns();
  void cleanModifiedColumns();
  int insertDB() throws SQLException, RockException;
  int deleteDB() throws SQLException, RockException;
  int updateDB() throws SQLException, RockException;
  Object clone ();
}
