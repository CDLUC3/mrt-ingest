package org.cdlib.mrt.ingest;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
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
        //System.out.println(profileProperties.keySet());
        assertEquals("Merritt Test", profileProperties.getProperty("ProfileDescription"));
        Identifier id = new Identifier("profile");
        ProfileState ps = ProfileUtil.getProfile(id, f);
        assertEquals("merritt_test_content", ps.getProfileID().getValue());
        assertEquals("Merritt Test", ps.getProfileDescription());
        assertEquals(Identifier.Namespace.ARK.name(), ps.getIdentifierScheme().name());
        assertEquals(1, ps.getContactsEmail().size());
        assertEquals("test.email@test.edu", ps.getContactsEmail().get(0).getContactEmail());
        assertEquals("99999", ps.getIdentifierNamespace());
        assertEquals("ark:/99999/m5000000", ps.getCollection().firstElement());
        //assertContains(new ProfileState().OBJECTTYPE, ps.getObjectType());
        assertTrue(Arrays.asList(new ProfileState().OBJECTTYPE).contains(ps.getObjectType()));
        assertTrue(Arrays.asList(new ProfileState().OBJECTROLE).contains(ps.getObjectRole()));
        assertEquals("", ps.getAggregateType());
        assertFalse(Arrays.asList(new ProfileState().AGGREGATETYPE).contains(ps.getAggregateType()));
    }
}
