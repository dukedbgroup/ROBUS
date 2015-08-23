/**
 * 
 */
package edu.duke.cacheplanner.query;

import java.io.File;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

/**
 * select 'projections' from 'dataset' where 'selections'
 * @author mayuresh
 */
@SuppressWarnings("serial")
public class TPCHQuery extends AbstractQuery implements Serializable {

	protected String path;
	protected String cachedPath;
	protected double benefit;

	public TPCHQuery(int queryId, int queueId, String path, String cachedPath, double benefit) {
		this.queryID = queryId;
		this.queueID = queueId;
		this.path = path;
		this.cachedPath = cachedPath;
		this.benefit = benefit;
	}

        /**
	 * @return the path 
	 */
	public String getPath() {
		return path;
	}

	/**
	 * @return the cachedPath
	 */
	public String getCachedPath() {
		return cachedPath;
	}

	/**
	 * @param path the path to set
	 */
	public void setPath(String path) {
		this.path = path;
	}

	/**
	 * @param cachedPath the cachedPath to set
	 */
	public void setCachedPath(String path) {
		this.cachedPath = path;
	}

	/**
	 * Returns the benefit a scan query would get when allocated cache for the dataset it needs
	 * @return scanBenefit 
	 */
	public double getBenefit() {
		return benefit;
	}

	public void setBenefit(double benefit) {
		this.benefit = benefit;
	}

	protected String readFromFile(String path) {
		File file = new File(path);
    		StringBuilder fileContents = new StringBuilder((int)file.length());
    		Scanner scanner = null;
		String lineSeparator = System.getProperty("line.separator");

		try {
			scanner = new Scanner(file);
        		while(scanner.hasNextLine()) {        
            			fileContents.append(scanner.nextLine() + lineSeparator);
			}
			return fileContents.toString();
    		} catch(Exception e) {
			System.out.println(e.getMessage());
		} finally {
        		scanner.close();
 		}
		return fileContents.toString();
	}

	@Override
	public String toHiveQL(Boolean cached) {
		if(cached) {
			return readFromFile(cachedPath);
		} else {
			return readFromFile(path);
		} 
	}
	
}
