/**
 * ----------------------------------------------------------------------- *
 * Copyright (C) 2010 LM Ericsson Limited. All rights reserved. *
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.repository.dbusers;

import java.util.*;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.ericsson.eniq.repository.DBUsersGet;
import com.ericsson.eniq.repository.ETLCServerProperties;

/**
 * 
 * @author etogust
 * 
 */
final public class GetDBPass {

    private GetDBPass() {
    }

    /**
     * Replaces string from file as crypted string
     */
    public static void main(final String[] args) {

        if (args == null || args.length != 2) {
            System.out.println("Invalid parameters. Need to pass <user> <connectionType>");
            System.exit(1);
        }
        final String requestedUser = args[0];
        final String reqConn = args[1];

        try {
            final Properties props = new ETLCServerProperties();
            if (requestedUser.equalsIgnoreCase(props.getProperty(ETLCServerProperties.DBUSERNAME))) {
                final String pwd = props.getProperty(ETLCServerProperties.DBPASSWORD);
                if(pwd == "" || pwd == null){
                	System.out.println("Empty string result");
                    System.exit(6);
                }
                System.out.print(pwd);
                return;
            }
        } catch (Exception e) {
            System.out.println("Problem reading properties.");
            System.out.println(e.getMessage());
            System.exit(5);
        }

        final List<Meta_databases> databases = new ArrayList<Meta_databases>();
        try {
            databases.addAll(DBUsersGet.getMetaDatabases(requestedUser, reqConn));
            if (databases.size() == 1) {
            	if(databases.get(0).getPassword() == "" || databases.get(0).getPassword() == null){
                	System.out.println("Empty string result");
                    System.exit(6);
                }else{
                	System.out.print(databases.get(0).getPassword());
                }
            } else {
                System.out.println("Could not find from database");
                System.exit(3);
            }
        } catch (Exception e) {
            System.out.println("Could not find from database");
            System.out.println(e.getMessage());
            System.exit(4);
        } finally {
            for (Meta_databases database : databases) {
                try {
                    RockFactory rockFact = database.getRockFactory();
                    if (rockFact != null) {
                        if (rockFact.getConnection() != null) {
                            rockFact.getConnection().close();
                        }
                    }
                } catch (Exception ex) {
                    System.out.println("Exception thrown while closing repdb connection...");
                    System.exit(7);
                }
            }
        }

    }
}
