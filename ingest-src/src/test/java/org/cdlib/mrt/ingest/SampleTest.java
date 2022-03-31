package org.cdlib.mrt.ingest;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Properties;

import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.core.Identifier;

public class SampleTest {
    @Test
    public void DemoTest() throws TException {
        ProfileFile pf = new ProfileFile();
        File f = new File("src/test/resources/merritt_test_content");
        Properties profileProperties = PropertiesUtil.loadFileProperties(f);
        System.out.println(profileProperties.keySet());
        assertEquals("Merritt Test", profileProperties.getProperty("ProfileDescription"));
        Identifier id = new Identifier("profile");
        ProfileState ps = ProfileUtil.getProfile(id, f);
    }
}
