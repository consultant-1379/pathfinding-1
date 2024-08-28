package com.distocraft.dc5000.etl.engine.common;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;
import java.util.Vector;

/**
 * Sends e-mail. Create an instance of eMail using default constructor or other
 * constructors. If used default constructor set needed values (mailserver,
 * port, recipients, message) If mail is sent out, set domain to be same as the
 * end in e-mail adress. Then just call sendMail().
 * 
 * original author Kari Hippolin
 * 
 * @author $Author: savinen $
 * @since JDK1.2
 */
public class EMail {

	private String myMailServer = null;
	private int myPort = 0;
	private String myDomain = null;
	private String mySenderName = null;
	private String mySenderAddress = null;
	private List<String> myRecipients = new Vector<String>();
	private String mySubject = null;
	private String myMessage = null;

	/**
	 * Default constructor
	 */
	public EMail() {

	}

	/**
	 * Constructor with spcefied values
	 * 
	 * @param server name of senders mailserver
	 * @param port mail servers port
	 * @param domain what is senders domain (same as end in senders e-mail)
	 * @param sender senders name
	 * @param address senders e-mail address
	 * @param recipient to whom mail is sent to
	 * @param subject mails subject
	 * @param message mails body
	 */
	public EMail(final String server, final int port, final String domain, final String sender,
			final String address, final String recipient, final String subject, final String message) {

		this.myMailServer = server;
		this.myPort = port;
		this.myDomain = domain;
		this.mySenderName = sender;
		this.mySenderAddress = address;
		this.myRecipients.add(recipient);
		this.mySubject = subject;
		this.myMessage = message;
	}

	/**
	 * Constructor with specified values
	 * 
	 * @param server name of senders mailserver
	 * @param port mail servers port
	 * @param domain what is senders domain (same as end in senders e-mail)
	 * @param sender senders name
	 * @param address senders e-mail address
	 * @param recipient to whose mail is sent to
	 * @param subject mails subject
	 * @param message mails body
	 */
	public EMail(final String server, final int port, final String domain, final String sender,
			final String address, final List<String> recipients,final String subject, final String message) {

		this.myMailServer = server;
		this.myPort = port;
		this.myDomain = domain;
		this.mySenderName = sender;
		this.mySenderAddress = address;
		this.myRecipients = recipients;
		this.mySubject = subject;
		this.myMessage = message;
	}

	/**
	 * Sets mailserver
	 * 
	 * @param server senders mailserver
	 */
	public void setMailServer(final String server) {
		myMailServer = server;
	}

	/**
	 * Sets mailservers port
	 * 
	 * @param port mailservers port
	 */
	public void setPort(final int port) {
		myPort = port;
	}

	/**
	 * Sets domain
	 * 
	 * @param domain senders domain
	 */
	public void setDomain(final String domain) {
		myDomain = domain;
	}

	/**
	 * Sets senders name
	 * 
	 * @param sender senders name
	 */
	public void setSenderName(final String sender) {
		mySenderName = sender;
	}

	/**
	 * Sets senders e-mail address
	 * 
	 * @param address senders e-mail address
	 */
	public void setSenderAddress(final String address) {
		mySenderAddress = address;
	}

	/**
	 * Sets recipient
	 * 
	 * @param recipient to whom mail is sent to
	 */
	public void setRecipient(final String recipient) {
		myRecipients = new Vector<String>();
		myRecipients.add(recipient);
	}

	/**
	 * Sets recipients
	 * 
	 * @param recipients to who mail is sent to
	 */
	public void setRecipients(final List<String> recipients) {
		myRecipients = recipients;
	}

	/**
	 * Adds recipient to recipients list
	 * 
	 * @param recipient to who mail is sent to
	 */
	public void addRecipient(final String recipient) {
		myRecipients.add(recipient);
	}

	/**
	 * Sets mails subject
	 * 
	 * @param subject sobject of mail
	 */
	public void setSubject(final String subject) {
		mySubject = subject;
	}

	/**
	 * Sets message
	 * 
	 * @param message mails body
	 */
	public void setMessage(final String message) {
		myMessage = message;
	}

	/**
	 * Sends mail. Constructs mail to be sent and sends it.
	 * 
	 * @returns true if mail is sent
	 * 
	 * @returns false if all necessary parameters are not set
	 */
	public boolean sendMail() {

		if (!((myMailServer == null) || (myPort == 0) || (myMessage == null) || myRecipients.isEmpty())) {

			try {

				// Opens connection
				final Socket s = new Socket(myMailServer, myPort);

				final BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), "8859_1"));
				final PrintWriter ps = new PrintWriter(s.getOutputStream(), true);

				ps.println("HELO " + myDomain);
				in.readLine();

				ps.println("MAIL FROM: <" + mySenderAddress + ">");
				in.readLine();

				for(String recipient : myRecipients) {
					ps.println("RCPT TO: " + recipient);
					in.readLine();
				}

				ps.println("DATA");
				in.readLine();
				ps.println("Subject: " + mySubject);
				ps.println("From: " + mySenderName + " <" + mySenderAddress + ">");
				ps.println("\n");
				ps.println(myMessage);
				ps.println("\n.\n");

				ps.println("QUIT");
				in.readLine();

				// Closes connection
				s.close();

			}

			catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}

		else {
			return false;
		}

		return true;

	}

}
