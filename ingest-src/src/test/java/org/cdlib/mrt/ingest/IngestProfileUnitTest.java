package org.cdlib.mrt.ingest;

import static org.junit.Assert.*;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.TException;
import org.junit.Test;

public class IngestProfileUnitTest extends IngestTestCore {
        public Date jan1_2022() {
                Calendar cal = Calendar.getInstance();
                cal.set(122, 1, 1);
                return cal.getTime();
        }

        @Test
        public void ReadProfileFile() throws TException, MalformedURLException {
                ProfileState ps = getProfileState();
                assertEquals("merritt_test_content", ps.getProfileID().getValue());
                assertEquals("Merritt Test", ps.getProfileDescription());
                assertEquals(Identifier.Namespace.ARK.name(), ps.getIdentifierScheme().name());
                assertEquals("99999", ps.getIdentifierNamespace());
                assertEquals("ark:/99999/m5000000", ps.getCollection().firstElement());
                // assertContains(new ProfileState().OBJECTTYPE, ps.getObjectType());
                assertTrue(Arrays.asList(new ProfileState().OBJECTTYPE).contains(ps.getObjectType()));
                assertTrue(Arrays.asList(new ProfileState().OBJECTROLE).contains(ps.getObjectRole()));
                assertEquals("", ps.getAggregateType());
                assertFalse(Arrays.asList(new ProfileState().AGGREGATETYPE).contains(ps.getAggregateType()));
                assertEquals("ark:/99999/j2000000", ps.getOwner());
                assertEquals(1, ps.getContactsEmail().size());
                assertEquals("test.email@test.edu", ps.getContactsEmail().get(0).getContactEmail());
                assertEquals(2, ps.getBatchProcessHandlers().size());
                assertEquals(1, ps.getBatchReportHandlers().size());
                assertEquals(3, ps.getQueueHandlers().size());
                assertEquals(4, ps.getInitializeHandlers().size());
                assertEquals(1, ps.getEstimateHandlers().size());
                assertEquals(1, ps.getProvisionHandlers().size());
                assertEquals(1, ps.getDownloadHandlers().size());
                assertEquals(7, ps.getProcessHandlers().size());
                assertEquals(1, ps.getRecordHandlers().size());
                assertEquals(2, ps.getNotifyHandlers().size());
                assertEquals(9999, ps.getTargetStorage().getNodeID());
                assertEquals("03", ps.getPriority());
                assertTrue(ps.getCreationDate().getDate().after(jan1_2022()));
                assertTrue(ps.getModificationDate().getDate().after(jan1_2022()));
                assertEquals("http://localhost:4567/shoulder/ark:/99999/fk4", ps.getObjectMinterURL().toString());
                assertEquals("additional", ps.getNotificationType());
                assertEquals("merritt_test", ps.getContext());
                assertNull(ps.getAccessURL());
                assertNull(ps.getLocalIDURL());
                assertNull(ps.getPURL());
        }

}
