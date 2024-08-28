/**
 * ----------------------------------------------------------------------- *
 * Copyright (C) 2010 LM Ericsson Limited. All rights reserved. *
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.repository.dbusers;

import java.util.List;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.ericsson.eniq.repository.DBUsersGet;

/**
 *
 * @author esuramo
 *
 */
final public class GetDBConnectionString {


  private GetDBConnectionString() {
  }


  public static void main(final String[] args) {

      if (args == null || args.length != 2) {
      System.out.println("Invalid parameters. Need to pass <USERNAME> <CONNECTION_NAME>");
      System.exit(1);
    }
    final String requestedUser = args[0];
    final String reqConn = args[1];

    try {
      final List<Meta_databases> databases = DBUsersGet.getMetaDatabases(requestedUser, reqConn);
      if (databases.size() == 1) {
        System.out.println(databases.get(0).getConnection_string());
      } else {
        System.out.println("Could not find from database");
        System.exit(3);
      }
    } catch (Exception e) {
      System.out.println("Could not find from database");
      System.out.println(e.getMessage());
      System.exit(4);
    }

  }
}
