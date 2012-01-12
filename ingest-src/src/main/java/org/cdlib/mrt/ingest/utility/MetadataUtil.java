/*
Copyright (c) 2011, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************/
package org.cdlib.mrt.ingest.utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 * simple metadata tool
 * @author mreyes
 */
public class MetadataUtil
{

    private static final String NAME = "MetadataUtil";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;
    private static final String DELIMITER = "\t";
    private LoggerInf logger = null;

    /**
     * write metadata to anvl file
     *
     * @param file source file (usually "mrt-ingest.txt")
     * @param properties map of properties
     * @return successful in writing resource map
     */
    public static boolean writeMetadataANVL(File file, Map<String, Object> properties, boolean append)
        throws TException
    {
	
	BufferedWriter fileBuffer = null;
	try {
	    fileBuffer = new BufferedWriter(new FileWriter(file, append));
	    Iterator ingestItr = properties.keySet().iterator();

	    while (ingestItr.hasNext()) {
		String key = (String) ingestItr.next();
		Object o = properties.get(key);
		String type = o.getClass().getName();
		if (type.equals(String.class.getName())) {
		    String value = (String) properties.get(key);
		    fileBuffer.write(key + ":" + DELIMITER + value);
		    // handle string...
		} else if (type.equals(ArrayList.class.getName())) {
		    // handle array-list...
		    ArrayList valueList = (ArrayList) properties.get(key);
		    Iterator iterator = valueList.iterator();
	            while (iterator.hasNext()) {
		        String value = (String) iterator.next();
		        fileBuffer.write(key + ":" + DELIMITER + value + "\n");
		    }
		} else {
		    // do not process
		}

		if (! type.equals(ArrayList.class.getName())) fileBuffer.newLine();
	    }
	} catch (Exception e) {
	}
	finally {
	    try {
	        fileBuffer.close();
	    } catch (Exception e) {
	    }
	}

	return true;
    }

    /**
     * read metadata anvl file
     *
     * @param ingestFile source file (usually "mrt-ingest.txt")
     * @return properties map of properties
     */
    public static Map<String, String> readMetadataANVL(File ingestFile)
        throws TException
    {
	
	Map linkedHashMap = new LinkedHashMap();
	BufferedReader fileBuffer = null;
	try {
	    fileBuffer = new BufferedReader(new InputStreamReader(new FileInputStream(ingestFile), "UTF-8")); 
	    Pattern dcPattern = Pattern.compile("who.*:.*|what.*:.*|when.*:.*|where.*:.*");
	    Pattern splitPattern = Pattern.compile(":");

	    String line = null;
	    String tokens[] = null;
	    while ((line = fileBuffer.readLine()) != null) {
		if (dcPattern.matcher(line).matches()) {
		    tokens = splitPattern.split(line, 2);
		    System.out.println("Found ANVL data: " + tokens[0] + " - " + tokens[1]);
		    if (StringUtil.isNotEmpty(StringUtil.squeeze(tokens[1]))) {
		        linkedHashMap.put(tokens[0], tokens[1]);
		    }
		} else {
		    System.out.println("No match: " + line);
		}
	    }

	} catch (Exception e) {
	}
	finally {
	    try {
	    } catch (Exception e) {
	    }
	}
	return linkedHashMap;
    }

    /**
     * read mom anvl file
     *
     * @param merritt object model file source file (usually "mrt-mom.txt")
     * @return properties map of properties
     */
    public static Map<String, Object> readMomANVL(File momFile)
        throws TException
    {

        Map linkedHashMap = new LinkedHashMap();
        BufferedReader fileBuffer = null;
        try {
            fileBuffer = new BufferedReader(new InputStreamReader(new FileInputStream(momFile), "UTF-8"));
            Pattern dcPattern = Pattern.compile(".*:.*");
            Pattern splitPattern = Pattern.compile(":");

            String line = null;
            String tokens[] = null;
            while ((line = fileBuffer.readLine()) != null) {
                if (dcPattern.matcher(line).matches()) {
                    tokens = splitPattern.split(line, 2);
                    System.out.println("Found ANVL data: " + tokens[0] + " - " + tokens[1]);
                    if (StringUtil.isNotEmpty(StringUtil.squeeze(tokens[1]))) {
                        linkedHashMap.put(tokens[0], tokens[1]);
                    }
                } else {
                    System.out.println("No match: " + line);
                }
            }

        } catch (Exception e) {
        }
        finally {
            try {
            } catch (Exception e) {
            }
        }
        return linkedHashMap;
    }

}
