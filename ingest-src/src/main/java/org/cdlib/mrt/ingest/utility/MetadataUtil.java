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
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
 
import org.cdlib.mrt.utility.DOMParser;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.XMLUtil;

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
	try {
	    writeMetadataANVL(file, properties, null, append);
	} catch (Exception e) {
		e.printStackTrace();
	}

	return true;
    }


    /**
     * write metadata to anvl file
     *
     * @param file source file (usually "mrt-ingest.txt")
     * @param properties map of properties
     * @return successful in writing resource map
     */
    public static boolean writeMetadataANVL(File file, Map<String, Object> properties, String delimiter, boolean append)
        throws TException
    {
	BufferedWriter fileBuffer = null;
	try {
	    if (delimiter == null) {
		System.out.println("No ANVL delimiter set - using default");
		delimiter = DELIMITER;
	    } else {
		System.out.println("ANVL delimiter: " + delimiter);
	    }
	    fileBuffer = new BufferedWriter(new FileWriter(file, append));
	    Iterator ingestItr = properties.keySet().iterator();

	    while (ingestItr.hasNext()) {
		String key = (String) ingestItr.next();
		Object o = properties.get(key);
		String type = o.getClass().getName();
		if (type.equals(String.class.getName())) {
		    String value = (String) properties.get(key);
		    fileBuffer.write(key + ":" + delimiter + value);
		    // handle string...
		} else if (type.equals(ArrayList.class.getName())) {
		    // handle array-list...
		    ArrayList valueList = (ArrayList) properties.get(key);
		    Iterator iterator = valueList.iterator();
	            while (iterator.hasNext()) {
		        String value = (String) iterator.next();
		        fileBuffer.write(key + ":" + delimiter + value + "\n");
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
    public static Map<String, String> readMetadataANVL(File ingestFile, int metadataDisplaySize)
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
	    boolean foundPrimary = false;
	    while ((line = fileBuffer.readLine()) != null) {
		if (dcPattern.matcher(line).matches()) {
		    tokens = splitPattern.split(line, 2);
		    System.out.println("[info] " + NAME + " Found ANVL data: " + tokens[0] + " - " + tokens[1]);

                    // a little hack to process local/primary IDs
                    if (tokens[0].matches("where")) {
                        if (tokens[1].contains("ark:/") && ! foundPrimary) {
			    tokens[0] = "where-primary";
			    foundPrimary = true;
                        } else tokens[0] = "where-local";
                    }
		    if (StringUtil.isNotEmpty(StringUtil.squeeze(tokens[1]))) {
			if (tokens[1].length() > metadataDisplaySize) {
		    	   System.out.println("[info] " + NAME + " Truncating metadata: " + tokens[0] + " to size: " + metadataDisplaySize + " ---- " + tokens[1].substring(0, metadataDisplaySize));
		           linkedHashMap.put(tokens[0], tokens[1].substring(0, metadataDisplaySize));
			} else {
		           linkedHashMap.put(tokens[0], tokens[1]);
			}
		    }
		} else {
		    System.out.println("[warn] " + NAME + "No match: " + line);
		}
	    }

	} catch (Exception e) {
	}
	finally {
	    try {
	        fileBuffer.close();
	    } catch (Exception e) { }
	}
	return linkedHashMap;
    }

    /**
     * read embargo anvl file
     *
     * @param merritt embargo fie
     * @return properties map of properties
     */
    public static Map<String, String> readEmbargoANVL(File embargoFile)
        throws TException
    {

        Map linkedHashMap = new LinkedHashMap();
        BufferedReader fileBuffer = null;
        try {
            fileBuffer = new BufferedReader(new InputStreamReader(new FileInputStream(embargoFile), "UTF-8"));
            // Pattern dcPattern = Pattern.compile("embargoEndDate.*:.*");
            Pattern dcPattern = Pattern.compile(".*:.*");
            Pattern splitPattern = Pattern.compile(":");

            String line = null;
            String tokens[] = null;
            while ((line = fileBuffer.readLine()) != null) {
                if (dcPattern.matcher(line).matches()) {
                    tokens = splitPattern.split(line, 2);
                    System.out.println("[info] " + NAME + " Found ANVL data: " + tokens[0] + " - " + tokens[1]);
                    if (StringUtil.isNotEmpty(StringUtil.squeeze(tokens[1]))) {
                        linkedHashMap.put(tokens[0], tokens[1]);
                    }
                } else {
                    System.out.println("[warn] " + NAME + " No embargo key/value pair defined: " + line);
                }
            }
        } 
	catch (Exception e) { }
        finally {
            try { 
	        fileBuffer.close();
	    } catch (Exception e) { }
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
                    System.out.println("[info] " + NAME + "Found ANVL data: " + tokens[0] + " - " + tokens[1]);
                    if (StringUtil.isNotEmpty(StringUtil.squeeze(tokens[1]))) {
                        linkedHashMap.put(tokens[0], tokens[1]);
                    }
                } else {
                    System.out.println("[warn] " + NAME + "No match: " + line);
                }
            }

        } 
	catch (Exception e) { }
        finally {
            try { 
	        fileBuffer.close();
	    } catch (Exception e) { }
        }
        return linkedHashMap;
    }

    /**
     * read DC xml file
     *
     * @param merritt DC source file (usually "mrt-dc.xml")
     * @return properties map of properties
     */
    public static Map<String, String> readDublinCoreXML(File DCFile)
    {

	String DC_DELIMITER = "; ";

        Map linkedHashMap = new LinkedHashMap();
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(DCFile);
	    Document document = DOMParser.doParse(fileInputStream, null);

	    System.out.println("[info] " + NAME + "Root element :" + document.getDocumentElement().getNodeName());
	    NodeList nodeList = document.getFirstChild().getChildNodes();
 
	    for (int temp = 0; temp < nodeList.getLength(); temp++) {
		Node node = nodeList.item(temp);
		if (node.getNodeType() == Node.ELEMENT_NODE) {
		    Element element = (Element) node;
 
		    String key = element.getTagName().replace(':', '.');
		    String value = element.getTextContent();
		    if (validDC(key)) {
			if (linkedHashMap.containsKey(key)) {
		            if (DEBUG) System.out.println("[info] " + NAME + " appending DC element: " + key + " - " + value);
			    linkedHashMap.put(key, linkedHashMap.get(key) + DC_DELIMITER + value);
			} else {
		            if (DEBUG) System.out.println("[info] " + NAME + " processing DC element: " + key + " - " + value);
			    linkedHashMap.put(key, value);
			}
		    } else {
		        if (DEBUG) System.out.println("[warn] " + NAME + " DC element not recognized: " + key);
		    }
	        }
	    }

        } catch (Exception e) { 
            if (DEBUG) System.out.println("[error] " + MESSAGE + ": unable to read mrt-dc.xml: " + DCFile.getName());
        } finally {
            try { 
	        fileInputStream.close();
	    } catch (Exception e) { }
        }
        return linkedHashMap;
    }


    /**
     * write DC xml file
     *
     * @param map defining Dublin Core
     * @param merritt DC source file (usually "mrt-dc.xml")
     * @return creation/upgrade status
     */
    public static void writeDublinCoreXML(Map<String, String> linkedHashMap, File DCFile)
        throws TException
    {
	String DC_DELIMITER = ";";

        try {
	    DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
	    DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
 
	    // root element
	    Document doc = docBuilder.newDocument();
	    Element rootElement = doc.createElement("DublinCore");
	    doc.appendChild(rootElement);

	    // namespace
	    rootElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
	    rootElement.setAttribute("xmlns:dc", "http://purl.org/dc/elements/1.1/");

	    // iterate over elements
            Iterator iterator = linkedHashMap.keySet().iterator();
	    while (iterator.hasNext()) {
		String key = (String) iterator.next();
		String value = (String) linkedHashMap.get(key);
        	if ( ! validDC(key)) {
	            System.out.println("[warn] " + NAME + " DC element not recognized: " + key);
		    continue;
	        }

		if (key.equals("dc.subject") || key.equals("dc.creator") || key.equals("dc.relation") || key.equals("dc.identifier")) {
		    // repeatable
		    for (String entry : (String []) value.split(";")) {
			Element element = doc.createElement("dc:" + key.toLowerCase().replace("dc.", ""));
	    	        element.setTextContent(XMLUtil.encodeValue(entry));
		        rootElement.appendChild(element);
		    }
		} else {
		    // non repeatable
		    Element element = doc.createElement("dc:" + key.toLowerCase().replace("dc.", ""));
	    	    element.setTextContent(XMLUtil.encodeValue(value));
		    rootElement.appendChild(element);
		}
	    }
 
	    // write the content into xml file
	    TransformerFactory transformerFactory = TransformerFactory.newInstance();
	    Transformer transformer = transformerFactory.newTransformer();
	    DOMSource source = new DOMSource(doc);
	    StreamResult result = new StreamResult(DCFile);
	    // StreamResult result = new StreamResult(System.out);		// Output to console for testing
	    transformer.transform(source, result);
 
        } catch (Exception e) { 
            throw new TException.GENERAL_EXCEPTION("[error] " +
                MESSAGE + ": unable to write mrt-dc.xml: " + DCFile.getName());
        } finally {
            try { 
	    } catch (Exception e) { }
        }

	return;
    }

 
    private static boolean validDC(String dcString) {
	String[] dcKeys = {"dc.contributor", "dc.coverage", "dc.creator", "dc.date", "dc.description", "dc.format",
	    "dc.identifier", "dc.language", "dc.publisher", "dc.relation", "dc.rights", "dc.source", "dc.subject",
	    "dc.title", "dc.type"};

	for (int i=dcKeys.length-1; 0 <= i; i--) {
	    if (dcKeys[i].equals(dcString)) return true;
	}

	return false;
    }


    /**
     * read DataCite xml file
     *
     * @param merritt DataCite source file (usually "mrt-datacite.xml")
     * @return properties map of properties
     */
    public static Map<String, String> readDataCiteXML(File DataCiteFile)
        throws TException
    {

	String DC_DELIMITER = "; ";

        Map linkedHashMap = new LinkedHashMap();
        FileInputStream fileInputStream = null;
        try {
	    String key = null;
	    String value = null;
            fileInputStream = new FileInputStream(DataCiteFile);
	    Document document = DOMParser.doParse(fileInputStream, null);

	    key = "datacite.creator";
	    XPathFactory xpathFactory = XPathFactory.newInstance();
	    XPath xpath = xpathFactory.newXPath();
	    XPathExpression expr = xpath.compile("/*[local-name()='resource']/*[local-name()='creators']/*[local-name()='creator']/*[local-name()='creatorName']");
	    NodeList nodeList = (NodeList) expr.evaluate(document, XPathConstants.NODESET);

	    for (int i=0; i < nodeList.getLength(); i++) {
		Node node = nodeList.item(i);
		value = node.getTextContent();

		if (linkedHashMap.containsKey(key)) {
		    if (DEBUG) System.out.println("[info] " + NAME + " appending DataCite element: " + key + " - " + value);
		    linkedHashMap.put(key, linkedHashMap.get(key) + DC_DELIMITER + value);
		} else {
		    if (DEBUG) System.out.println("[info] " + NAME + " processing DataCite element: " + key + " - " + value);
		    linkedHashMap.put(key, value);
		}

	    }

	    key = "datacite.title";
	    expr = xpath.compile("/*[local-name()='resource']/*[local-name()='titles']/*[local-name()='title']");
	    nodeList = (NodeList) expr.evaluate(document, XPathConstants.NODESET);

	    for (int i=0; i < nodeList.getLength(); i++) {
		Node node = nodeList.item(i);
		value = node.getTextContent();

		if (linkedHashMap.containsKey(key)) {
		    if (DEBUG) System.out.println("[info] " + NAME + " appending DataCite element: " + key + " - " + value);
		    linkedHashMap.put(key, linkedHashMap.get(key) + DC_DELIMITER + value);
		} else {
		    if (DEBUG) System.out.println("[info] " + NAME + " processing DataCite element: " + key + " - " + value);
		    linkedHashMap.put(key, value);
		}

	    }

	    key = "datacite.publicationyear";
	    expr = xpath.compile("//*[local-name()='resource']/*[local-name()='publicationYear']");
	    value = expr.evaluate(document);
	    if (DEBUG) System.out.println("[info] " + NAME + " appending DataCite element: " + key + " - " + value);
	    linkedHashMap.put(key, value);

        } catch (Exception e) { 
	    e.printStackTrace();
            if (DEBUG) System.out.println("[error] " + MESSAGE + ": unable to read mrt-datacite.xml: " + e.getMessage());
            throw new TException.GENERAL_EXCEPTION("[error] " +
                MESSAGE + ": unable to process mrt-datacite.xml: " + e.getMessage());
        } finally {
            try { 
	        fileInputStream.close();
	    } catch (Exception e) { }
        }
        return linkedHashMap;
    }
}
