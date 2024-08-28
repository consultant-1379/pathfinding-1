package com.distocraft.dc5000.etl.engine.plugin;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * Class PluginLoader Reads & loads java classes from plugin directory It
 * constructed once from TransferEngine at the start of the dagger
 * TransferEngine.
 * 
 */
public class PluginLoader {
	
	// the directory of available plugin classes
	private final String pluginPath;
	// Loaded classes & jars
	private final URLClassLoader urlCl;

	/**
	 * Constructor
	 * 
	 * @param String
	 *          pluginPath the directory of available plugin classes
	 */
	public PluginLoader(final String pluginPath) throws IOException {
		this.pluginPath = pluginPath;
		// Save class loader so that we can restore later
		final ClassLoader prevCl = Thread.currentThread().getContextClassLoader();

		// Create class loader using given URL
		// Use prevCl as parent to maintain current visibility
		this.urlCl = URLClassLoader.newInstance(this.getUrls(), prevCl);

		Thread.currentThread().setContextClassLoader(urlCl);

	}

	/**
	 * Loads a class
	 * 
	 * @param name
	 *          of the class
	 * @return the class
	 * @exception could
	 *              not find given class
	 */
	public Class<?> loadClass(final String className) throws ClassNotFoundException {
		return this.urlCl.loadClass(className);
	}

	/**
	 * Returns the class names in the plugin directory
	 */
	public String[] getPluginNames() {
		final File classFile = new File(this.pluginPath);

		final String[] fileList = classFile.list();
		int classes = 0;
		for (String file : fileList) {
			if (file.toUpperCase().indexOf("PLUG.CLASS") > 0) {
				classes++;
			}
		}

		String[] classFileList = new String[classes];

		int counter = 0;
		for (String plugName : fileList) {
			if (plugName.toUpperCase().indexOf("PLUG.CLASS") > 0) {
				classFileList[counter] = plugName.substring(0, plugName.lastIndexOf("."));
				counter++;
			}
		}
		return classFileList;
	}

	/**
	 * Returns the class names in the plugin directory
	 */
	public String[] getPluginMethodNames(final String pluginName, final boolean isGetGetMethod, final boolean isGetSetMethod)
			throws ClassNotFoundException {
		
		final Class<?> pluginClass = this.loadClass(pluginName);
		final Method[] pluginMethods = pluginClass.getMethods();

		final List<String> selectedMethodNames = new ArrayList<String>();
		for (Method method : pluginMethods) {		
			final String methodName = method.getName();

			if ((isGetGetMethod && methodName.indexOf("get") >= 0) || (isGetSetMethod && methodName.indexOf("set") >= 0)) {
				selectedMethodNames.add(methodName);
			}
		}

		return selectedMethodNames.toArray(new String[selectedMethodNames.size()]);

	}

	/**
	 * Returns the method parameters separated with ,
	 * 
	 * @param String
	 *          pluginName The plugin to load
	 * @param String
	 *          methodName The method that hold the parameters
	 * @return String
	 */
	public String getPluginMethodParameters(final String pluginName, final String methodName) throws ClassNotFoundException {

		final Class<?> pluginClass = this.loadClass(pluginName);
		final Method[] pluginMethods = pluginClass.getMethods();

		for (Method method : pluginMethods) {		
			if (methodName.equals(method.getName())) {

				final Class<?>[] classes = method.getParameterTypes();

				if (classes.length > 0) {
					return classesToString(classes);
				}
			}

		}

		return "";

	}

	/**
	 * Returns the constructor parameters separated with ,
	 * 
	 * @param String
	 *          pluginName The plugin to load
	 * @return String
	 */
	public String getPluginConstructorParameters(final String pluginName) throws ClassNotFoundException {

		final Class<?> pluginClass = this.loadClass(pluginName);
		final Constructor<?>[] pluginConstructors = pluginClass.getConstructors();

		for (Constructor<?> constructor : pluginConstructors) {
			final Class<?>[] classes = constructor.getParameterTypes();

			if (classes.length > 0) {
				return classesToString(classes);
			}

		}

		return "";

	}

	/**
	 * Return a string containing the class names separated with a ,
	 * 
	 * @return String
	 */
	private String classesToString(final Class<?>[] classes) {

		String retString = "";

		for (int i = 0; i < classes.length; i++) {
			if (retString.length() > 0) {
				retString += ",";
			}
			final String tempStr = classes[i].toString();
			retString += tempStr.substring(tempStr.lastIndexOf(".") + 1, tempStr.length());
		}
		if (retString.lastIndexOf(";") >= 0) {
			retString = retString.substring(0, retString.lastIndexOf(";"));
		}
		return retString;
	}

	private URL[] getUrls() throws MalformedURLException {

		final List<URL> temp = new ArrayList<URL>();
		
		final File tempDir = new File(this.pluginPath);
		if (tempDir.exists()) {
			final String[] filesInDir = tempDir.list();

			for (String file : filesInDir) {

				if (file.toUpperCase().indexOf(".JAR") >= 0) {
					// Save class loader so that we can restore later
					temp.add(new URL("file:" + this.pluginPath + "/" + file));
				}

			}
			temp.add(new URL("file:" + this.pluginPath + "/"));

		}

		return temp.toArray(new URL[temp.size()]);
		
	}

}