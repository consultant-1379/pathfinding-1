package com.ericsson.eniq.Services;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Properties;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

/**
 * Class is for executing Command on Remote system and capture the results.
 * 
 * @author Rama Yaganti
 * 
 */
public final class RemoteExecutor {
  
  static int exitvalue = -1;

  // prevent instance creation
  private RemoteExecutor() {
  }

  /**
   * This command is support for executing any remote commands from GUI.
   * 
   * @param usernme
   *          of the remote host
   * @param password
   *          of the remote host
   * @param host
   *          Remote host address
   * @param cmd
   *          command that is needed to run
   * @return returns the output after the execution.
   * @throws JSchException
   * @throws IOException
   * @throws Exception
   */
  public static String executeComand(final String user, final String password, final String host, final String cmd)
      throws JSchException, IOException {

    final StringBuilder result = new StringBuilder(); // append output of
                                                      // executed command

    final Session session = getSession(user, host, password);

    session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
    session.connect();

    final Channel channel = session.openChannel("exec");

    ((ChannelExec) channel).setCommand(cmd);
    channel.setXForwarding(true);

    final InputStream in = channel.getInputStream();
    channel.connect();
    channel.setInputStream(System.in);
    byte[] tmp = new byte[1];
    char[] tmpC = new char[1];
    Charset charset = Charset.forName("US-ASCII");
    CharsetDecoder decoder = charset.newDecoder();

    // gather command output
    while (true) {

      while (in.available() > 0) {
        final int i = in.read(tmp, 0, 1);
        if (i < 0) {
          break;
        }
        CharBuffer cb = CharBuffer.wrap(tmpC);
        decoder.decode(ByteBuffer.wrap(tmp), cb, false);
        result.append(String.valueOf(cb.array()));
        cb = null;
      }
      // Close the channel after collecting
      if (channel.isClosed()) {
        in.close();
        break;
      }
    }

    channel.disconnect();
    session.disconnect();

    return result.toString();
  }
  
  /**
   * This command is support for executing any remote commands
   * This makes use of ssh key sharing which was delivered by NMI in 2.1.8 allowing for a secure connection without password prompt.
   *
   * @param usernme
   *          of the remote host
   * @param host
   *          Remote host address
   * @param cmd
   *          command that is needed to run
   * @return returns the output after the execution.
   * @throws JSchException
   * @throws IOException
   * @throws Exception
   */
  public static String executeComandSshKey(final String user, final String host, final String cmd) throws JSchException,
      IOException {

    final StringBuilder result = new StringBuilder(); // append output of
                                                      // executed command

    JSch jsch = new JSch();

    final String systemuser = System.getProperty("user.home");
    final String pathseparator = System.getProperty("file.separator");
    final String keyfile = systemuser + pathseparator + ".ssh" + pathseparator + "id_rsa";

    final File f = new File(keyfile);
    if (!f.exists()) {
      throw new IOException("File: " + keyfile + " does not exist.");
    }

    jsch.addIdentity(keyfile);

    Session session = jsch.getSession(user, host, 22);
    session.setConfig("StrictHostKeyChecking", "no");
    session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
    session.connect();

    final Channel channel = session.openChannel("exec");

    ((ChannelExec) channel).setCommand(cmd);
    channel.setXForwarding(true);

    final InputStream in = channel.getInputStream();
    channel.connect();
    channel.setInputStream(System.in);
    byte[] tmp = new byte[1];
    char[] tmpC = new char[1];
    int count = 0;
    Charset charset = Charset.forName("US-ASCII");
    CharsetDecoder decoder = charset.newDecoder();

    // gather command output
    while (true) {

      while (in.available() > 0) {
        final int i = in.read(tmp, 0, 1);
        if (i < 0) {
          break;
        }
        CharBuffer cb = CharBuffer.wrap(tmpC);
        decoder.decode(ByteBuffer.wrap(tmp), cb, false);
        result.append(String.valueOf(cb.array()));
        cb = null;
        count += 1;
      }
      // Close the channel after collecting
      if (channel.isClosed()) {
        exitvalue = channel.getExitStatus();
        in.close();
        break;
      }
    }

    channel.disconnect();
    session.disconnect();

    return result.toString();
  }

  /**
   * 
   * @param user
   * @param host
   * @param password
   * @return
   * @throws JSchException
   */
  private static Session getSession(final String user, final String host, String password) throws JSchException {
    Session session = new JSch().getSession(user, host, 22);
    session.setPassword(password);
    session.setUserInfo(new MyUserInfo());
    return session;
  }

  public static class MyUserInfo implements UserInfo {

    @Override
    public String getPassword() {
      return "password";
    }

    @Override
    public String getPassphrase() {
      return "";
    }

    @Override
    public boolean promptPassword(final String arg0) {
      return true;
    }

    @Override
    public boolean promptPassphrase(final String arg0) {
      return true;
    }

    @Override
    public boolean promptYesNo(final String arg0) {
      return true;
    }

    @Override
    public void showMessage(final String arg0) {
    }
  }

  public static void main(final String[] args) {

    if (args == null || args.length != 3) {
      System.out.println(args.length);
      System.out.println("Invalid parameters. Need to pass <USERNAME> <HOST> <COMMAND>");
      System.exit(1);
    }
    final String requestedUser = args[0];
    final String hostIP = args[1];
    final String excCommand = args[2];

    System.out.println("user:\t" + requestedUser);
    System.out.println("ip:\t" + hostIP);
    System.out.println("command: " + excCommand);

    try {

      String output = executeComandSshKey(requestedUser, hostIP, excCommand);
      if ((output == null) || (output.trim().length() <= 0)) {
        System.out.println("NO Result from command");
        System.exit(90);
      }
      System.out.println("Result output:\n" + output);
      System.out.println("Result return code: " + exitvalue);
      System.exit(exitvalue);

    } catch (JSchException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(2);
    }

  }

  
 //FFU Feature - EQEV-41980
/**
 * Validate SSH connection with given username and password
 * (Used to validate root password in FFU feature)
 * @param rootUsr
 * @param rootPwd
 * @param hostAdd
 * @return
 */
public static boolean validatePwd(String rootUsr, String rootPwd, String hostAdd) {
	Session session = null;
	try{
	  	Properties config = new Properties(); 
		config.put("StrictHostKeyChecking", "no");
		JSch jsch = new JSch();
		session = jsch.getSession(rootUsr, hostAdd);
		session.setPassword(rootPwd);
		session.setConfig(config);
		session.connect();
		}
	catch (Exception e){
		return false;
		}
	finally{
		session.disconnect();
		}
	return true;
	}
}

