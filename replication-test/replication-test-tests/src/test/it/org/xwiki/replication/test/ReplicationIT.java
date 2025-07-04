/*
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.xwiki.replication.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.xwiki.component.embed.EmbeddableComponentManager;
import org.xwiki.component.manager.ComponentLookupException;
import org.xwiki.component.manager.ComponentManager;
import org.xwiki.contrib.replication.entity.DocumentReplicationDirection;
import org.xwiki.contrib.replication.entity.DocumentReplicationLevel;
import org.xwiki.contrib.replication.internal.instance.ReplicationInstanceStore;
import org.xwiki.contrib.replication.internal.instance.StandardReplicationInstanceClassInitializer;
import org.xwiki.contrib.replication.test.po.PageReplicationAdministrationSectionPage;
import org.xwiki.contrib.replication.test.po.RegisteredInstancePane;
import org.xwiki.contrib.replication.test.po.ReplicationConflictPane;
import org.xwiki.contrib.replication.test.po.ReplicationDocExtraPane;
import org.xwiki.contrib.replication.test.po.ReplicationPage;
import org.xwiki.contrib.replication.test.po.RequestedInstancePane;
import org.xwiki.contrib.replication.test.po.RequestingInstancePane;
import org.xwiki.contrib.replication.test.po.WikiReplicationAdministrationSectionPage;
import org.xwiki.extension.ExtensionId;
import org.xwiki.extension.job.InstallRequest;
import org.xwiki.extension.job.internal.InstallJob;
import org.xwiki.like.test.po.LikeButton;
import org.xwiki.model.EntityType;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.model.reference.EntityReference;
import org.xwiki.model.reference.LocalDocumentReference;
import org.xwiki.model.reference.ObjectReference;
import org.xwiki.model.reference.WikiReference;
import org.xwiki.platform.wiki.creationjob.WikiCreationRequest;
import org.xwiki.platform.wiki.creationjob.internal.WikiCreationJob;
import org.xwiki.rest.XWikiRestException;
import org.xwiki.rest.internal.ModelFactory;
import org.xwiki.rest.model.jaxb.Attachment;
import org.xwiki.rest.model.jaxb.History;
import org.xwiki.rest.model.jaxb.HistorySummary;
import org.xwiki.rest.model.jaxb.Page;
import org.xwiki.rest.model.jaxb.Property;
import org.xwiki.rest.model.jaxb.Wiki;
import org.xwiki.rest.model.jaxb.Wikis;
import org.xwiki.rest.resources.attachments.AttachmentResource;
import org.xwiki.rest.resources.pages.PageResource;
import org.xwiki.test.docker.internal.junit5.JobExecutor;
import org.xwiki.test.docker.junit5.UITest;
import org.xwiki.test.integration.XWikiExecutor;
import org.xwiki.test.ui.AbstractTest;
import org.xwiki.test.ui.TestUtils;
import org.xwiki.test.ui.TestUtils.RestTestUtils;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.xwiki.replication.test.AllITs.INSTANCE_0;
import static org.xwiki.replication.test.AllITs.INSTANCE_0_2;
import static org.xwiki.replication.test.AllITs.INSTANCE_0_2_ENABLED;
import static org.xwiki.replication.test.AllITs.INSTANCE_1;
import static org.xwiki.replication.test.AllITs.INSTANCE_2;

/**
 * Verify the document cache update based on distributed events.
 * 
 * @version $Id: f6ae6de6d59b97c88b228130b45cd26ce7b305ff $
 */
@UITest(properties = {
    "xwikiPropertiesAdditionalProperties=test.prchecker.excludePattern=.*",
    "xwikiDbHbmCommonExtraMappings=notification-filter-preferences.hbm.xml"
    }
)
public class ReplicationIT extends AbstractTest
{
    private static final LocalDocumentReference SERVICE_SETCONFIGURATION_REFERENCE =
        new LocalDocumentReference("Test", "SetConfigurationBypassReplication");

    private static final LocalDocumentReference REPLICATION_ALL_BOTH =
        new LocalDocumentReference("ReplicationALL", "WebHome");

    private static final LocalDocumentReference REPLICATION_ALL_SEND_ONLY =
        new LocalDocumentReference("ReplicationALLSendOnly", "WebHome");

    private static final LocalDocumentReference REPLICATION_REFERENCE =
        new LocalDocumentReference("ReplicationREFERENCE", "WebHome");

    private static final String MAIN_WIKI = "xwiki";

    private static final String SUB_WIKI = "testwiki";

    private static final String CURRENT_INSTANCE = "Current instance";

    private static class Instance
    {
        private final int id;

        private final String name;

        private final String custom;

        private String displayName;

        private String uri;

        private String proxyURI;

        private WireMockRule proxy;

        private MappingBuilder proxyStubBuilder;

        private StubMapping proxyStub;

        public Instance(int id)
        {
            this.id = id;
            this.name = "Instance " + id;
            this.custom = "CUSTOM " + id;
        }

        public void setup(WireMockRule proxy) throws IOException, Exception
        {
            getUtil().switchExecutor(id);
            getUtil().gotoPage(new LocalDocumentReference("Main", "WebHome"));
            getUtil().loginAsSuperAdmin();
            this.uri = StringUtils.removeEnd(getUtil().getBaseURL(), "/");
            this.proxyURI = uri.replace(String.valueOf(8080 + this.id), String.valueOf(8070 + this.id));
            this.displayName = this.name + " (" + this.proxyURI + ")";
            this.proxy = proxy;
            this.proxyStubBuilder = WireMock.any(WireMock.urlMatching(".*"))
                .willReturn(WireMock.aResponse().proxiedFrom(StringUtils.removeEnd(uri, "/xwiki")));
            this.proxyStub = this.proxy.stubFor(this.proxyStubBuilder);

            // Install a service helper
            try (InputStream stream = getClass().getResourceAsStream("/setconfigurationbypassreplication.wiki")) {
                getUtil().rest().savePage(SERVICE_SETCONFIGURATION_REFERENCE,
                    IOUtils.toString(stream, StandardCharsets.UTF_8), "");
            }
        }
    }

    private static final Instance[] INSTANCES = new Instance[] {new Instance(0), new Instance(1), new Instance(2)};

    @ClassRule
    public static WireMockRule proxy0 = new WireMockRule(WireMockConfiguration.options().port(8070));

    @ClassRule
    public static WireMockRule proxy1 = new WireMockRule(WireMockConfiguration.options().port(8071));

    @ClassRule
    public static WireMockRule proxy2 = new WireMockRule(WireMockConfiguration.options().port(8072));

    private static final MappingBuilder proxyFailingBuilder =
        WireMock.any(WireMock.urlMatching(".*")).willReturn(WireMock.aResponse().withStatus(500));

    private static EmbeddableComponentManager fullComponentManager;

    private static <T> void assertEqualsWithTimeout(T expected, Supplier<T> supplier) throws InterruptedException
    {
        long t2;
        long t1 = System.currentTimeMillis();
        T result;
        while (!Objects.equals((result = supplier.get()), expected)) {
            t2 = System.currentTimeMillis();
            if (t2 - t1 > 10000L) {
                fail(String.format("Should have been [%s] but was [%s]", expected, result));
            }
            Thread.sleep(100L);
        }
    }

    private static void assertEqualsContentWithTimeout(LocalDocumentReference documentReference, String content)
        throws InterruptedException
    {
        assertEqualsWithTimeout(content, () -> {
            try {
                Page page = getUtil().rest().<Page>get(documentReference, false);

                return page != null ? page.getContent() : null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void assertEqualsVersionWithTimeout(LocalDocumentReference documentReference, String version)
        throws InterruptedException
    {
        assertEqualsWithTimeout(version, () -> {
            try {
                Page page = getUtil().rest().<Page>get(documentReference, false);

                return page != null ? page.getVersion() : null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void assertEqualsCustomWithTimeout(String uri, String value) throws InterruptedException
    {
        assertEqualsWithTimeout(value, () -> {
            try {
                return getCustom(uri, false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void assertEqualsHistorySizeWithTimeout(LocalDocumentReference documentReference, int historySize)
        throws InterruptedException
    {
        assertEqualsWithTimeout(historySize, () -> {
            try {
                return getHistory(documentReference).getHistorySummaries().size();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void assertDoesNotExistWithTimeout(LocalDocumentReference documentReference)
        throws InterruptedException
    {
        assertEqualsWithTimeout(null, () -> {
            try {
                return getUtil().rest().<Page>get(documentReference, false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private PageReplicationAdministrationSectionPage assertReplicationModeWithTimeout(EntityReference reference,
        String value) throws InterruptedException
    {
        String scope;
        if (reference.getType() == EntityType.SPACE) {
            scope = "space";
        } else {
            scope = "document";
        }

        assertEqualsWithTimeout(value, () -> {
            try {
                return PageReplicationAdministrationSectionPage.gotoPage(reference).getMode(scope);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return new PageReplicationAdministrationSectionPage();
    }

    private static WikiReplicationAdministrationSectionPage assertEqualsRequestingInstancesWithTimeout(
        int requestingInstances) throws InterruptedException
    {
        assertEqualsWithTimeout(requestingInstances, () -> {
            try {
                WikiReplicationAdministrationSectionPage admin = WikiReplicationAdministrationSectionPage.gotoPage();

                return admin.getRequestingInstances().size();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return new WikiReplicationAdministrationSectionPage();
    }

    private static WikiReplicationAdministrationSectionPage assertEqualsRequestedInstancesWithTimeout(
        int requestedInstances) throws InterruptedException
    {
        assertEqualsWithTimeout(requestedInstances, () -> {
            try {
                WikiReplicationAdministrationSectionPage admin = WikiReplicationAdministrationSectionPage.gotoPage();

                return admin.getRequestedInstances().size();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return new WikiReplicationAdministrationSectionPage();
    }

    private static ReplicationPage assertEqualsLikeWithTimeout(LocalDocumentReference documentReference, int likes)
        throws InterruptedException
    {
        assertEqualsWithTimeout(likes, () -> {
            try {
                getUtil().gotoPage(documentReference);
                LikeButton likeButton = new LikeButton();

                return likeButton.getLikeNumber();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return new ReplicationPage();
    }

    private static int getCurrentExecutorId()
    {
        XWikiExecutor currentExecutor = getUtil().getCurrentExecutor();

        List<XWikiExecutor> executors = getContext().getExecutors();
        for (int i = 0; i < executors.size(); ++i) {
            if (executors.get(i) == currentExecutor) {
                return i;
            }
        }

        throw new IllegalStateException("Current executor not found among the list of executors");
    }

    private static String getOwnerDisplayName(Integer owner)
    {
        if (owner == null || owner.intValue() == getCurrentExecutorId()) {
            return CURRENT_INSTANCE;
        }

        return INSTANCES[owner].displayName;
    }

    private static void assertDocumentWithTimeout(LocalDocumentReference documentReference, String content,
        String version, Integer owner, boolean readonly) throws Exception
    {
        assertEqualsContentWithTimeout(documentReference, content);
        Page page = getUtil().rest().<Page>get(documentReference);
        assertEquals("Wrong version in the replicated document", version, page.getVersion());
        ReplicationDocExtraPane replicationExtraPane =
            ReplicationPage.gotoPage(documentReference).openReplicationDocExtraPane();
        assertEquals(getOwnerDisplayName(owner), replicationExtraPane.getOwner());
        assertEquals(readonly, replicationExtraPane.isReadonly());
    }

    private static void assertDocumentREFERENCEWithTimeout(LocalDocumentReference documentReference, Integer owner)
        throws Exception
    {
        assertEqualsContentWithTimeout(documentReference, "");
        Page page = getUtil().rest().<Page>get(documentReference);
        assertEquals("Wrong version in the replicated document", "1.1", page.getVersion());
        ReplicationDocExtraPane replicationExtraPane =
            ReplicationPage.gotoPage(documentReference).openReplicationDocExtraPane();
        assertEquals(getOwnerDisplayName(owner), replicationExtraPane.getOwner());
        assertSame(DocumentReplicationLevel.REFERENCE, replicationExtraPane.getLevel());
    }

    private String getAttachmentContent(EntityReference attachmentReference) throws Exception
    {
        try (InputStream is = getUtil().rest().<InputStream>get(AttachmentResource.class, attachmentReference, false)) {
            if (is != null) {
                return IOUtils.toString(is, StandardCharsets.UTF_8);
            }

            return null;
        }
    }

    private Page getPageWithAttachments(EntityReference documentReference) throws Exception
    {
        String uri = getUtil().rest()
            .createUri(PageResource.class, Map.of(), getUtil().rest().toElements(documentReference)).toString();
        uri += "?attachments=true";

        return getUtil().rest().get(new URI(uri), documentReference);
    }

    private void saveMinor(Page page) throws Exception
    {
        Map<String, Object[]> queryParams = new HashMap<>();
        queryParams.put("minorRevision", new Object[] {Boolean.TRUE.toString()});

        TestUtils.assertStatusCodes(getUtil().rest().executePut(PageResource.class, page, queryParams,
            getUtil().getCurrentWiki(), page.getSpace(), page.getName()), true, TestUtils.STATUS_CREATED_ACCEPTED);
    }

    private static History getHistory(LocalDocumentReference documentReference) throws Exception
    {
        return getUtil().rest().getResource("/wikis/{wikiName}/spaces/{spaceName: .+}/pages/{pageName}/history", null,
            getUtil().getCurrentWiki(), documentReference.getParent().getName(), documentReference.getName());
    }

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        // Setup URLs and Wiremock
        INSTANCES[0].setup(proxy0);
        INSTANCES[1].setup(proxy1);
        INSTANCES[2].setup(proxy2);

        // Link instances
        linkInstances();

        // Make sure all instances have subwiki "testwiki"
        getUtil().switchExecutor(INSTANCE_0);
        setupWiki(SUB_WIKI);
        getUtil().switchExecutor(INSTANCE_1);
        setupWiki(SUB_WIKI);
        getUtil().switchExecutor(INSTANCE_2);
        setupWiki(SUB_WIKI);
    }

    private static void setupWiki(String wiki) throws Exception
    {
        // Check if the wiki already exist
        Wikis wikis = getUtil().rest().getResource("/wikis", null);
        for (Wiki restWiki : wikis.getWikis()) {
            if (restWiki.getName().equals(wiki)) {
                // The wiki already exist
                return;
            }
        }

        // Create the wiki
        createWiki(wiki);
        installReplication(wiki);
    }

    private static ComponentManager getFullComponentManager()
    {
        if (fullComponentManager == null) {
            fullComponentManager = new EmbeddableComponentManager();
            fullComponentManager.initialize(Thread.currentThread().getContextClassLoader());
        }

        return fullComponentManager;
    }

    private static ModelFactory getModelFactory() throws ComponentLookupException
    {
        return getFullComponentManager().getInstance(ModelFactory.class);
    }

    private static void installReplication(String wiki) throws XWikiRestException, ComponentLookupException, Exception
    {
        InstallRequest installRequest = new InstallRequest();
        installRequest.setInteractive(false);
        installRequest.addNamespace("wiki:" + wiki);
        installRequest.addExtension(new ExtensionId("org.xwiki.contrib.replication:replication-ui",
            System.getProperties().getProperty("replication.version")));
        installRequest.setProperty("user.reference", new DocumentReference("xwiki", "XWiki", "superadmin"));

        JobExecutor jobExecutor = new JobExecutor();
        jobExecutor.execute(InstallJob.JOBTYPE, getModelFactory().toRestJobRequest(installRequest),
            getUtil().rest().getBaseURL(), getUtil().getDefaultCredentials());
    }

    private static void createWiki(String wiki) throws Exception
    {
        WikiCreationRequest wikiRequest = new WikiCreationRequest();
        wikiRequest.setInteractive(false);
        wikiRequest.setWikiId(wiki);
        wikiRequest.setAlias(wiki);
        wikiRequest.setFailOnExist(true);

        JobExecutor jobExecutor = new JobExecutor();
        jobExecutor.execute(WikiCreationJob.JOB_TYPE, getModelFactory().toRestJobRequest(wikiRequest),
            getUtil().rest().getBaseURL(), getUtil().getDefaultCredentials());
    }

    private static void setCustom(String value) throws Exception
    {
        WikiReference mainWikiReference = new WikiReference("xwiki");
        DocumentReference mainWikiInstances =
            new DocumentReference(ReplicationInstanceStore.REPLICATION_INSTANCES, mainWikiReference);
        ObjectReference instanceReference =
            new ObjectReference(StandardReplicationInstanceClassInitializer.CLASS_FULLNAME + "[0]", mainWikiInstances);

        org.xwiki.rest.model.jaxb.Object object =
            (org.xwiki.rest.model.jaxb.Object) getUtil().rest().get(instanceReference);
        RestTestUtils.getProperty("custom", object, true).setValue(value);
        getUtil().rest().update(object);
    }

    private static String getCustom(String uri) throws Exception
    {
        return getCustom(uri, true);
    }

    private static String getCustom(String uri, boolean failIfNotFound) throws Exception
    {
        WikiReference mainWikiReference = new WikiReference("xwiki");
        DocumentReference mainWikiInstances =
            new DocumentReference(ReplicationInstanceStore.REPLICATION_INSTANCES, mainWikiReference);

        for (int i = 0; i < 10; ++i) {
            ObjectReference customReference = new ObjectReference(
                StandardReplicationInstanceClassInitializer.CLASS_FULLNAME + '[' + i + ']', mainWikiInstances);

            org.xwiki.rest.model.jaxb.Object object =
                (org.xwiki.rest.model.jaxb.Object) getUtil().rest().get(customReference, failIfNotFound);

            if (object != null) {
                Property property =
                    RestTestUtils.getProperty(StandardReplicationInstanceClassInitializer.FIELD_URI, object, false);

                if (property.getValue().equals(uri)) {
                    return RestTestUtils.getProperty("custom", object, false).getValue();
                }
            }
        }

        return null;
    }

    // Tests

    private static void linkInstances() throws Exception
    {
        // Configure instance0 name and URI
        getUtil().switchExecutor(INSTANCE_0);
        WikiReplicationAdministrationSectionPage admin0 = WikiReplicationAdministrationSectionPage.gotoPage();
        admin0.setCurrentName(INSTANCES[0].name);
        admin0.setCurrentURI(INSTANCES[0].proxyURI);
        admin0 = admin0.clickSaveButton();
        setCustom(INSTANCES[0].custom);
        assertEquals(INSTANCES[0].custom, getCustom(INSTANCES[0].proxyURI));
        // Make sure the other cluster member have the changes
        if (INSTANCE_0_2_ENABLED) {
            getUtil().switchExecutor(INSTANCE_0_2);
            admin0 = WikiReplicationAdministrationSectionPage.gotoPage();
            assertEquals(INSTANCES[0].name, admin0.getCurrentName());
            assertEquals(INSTANCES[0].proxyURI, admin0.getCurrentURI());
        }
        // Configure instance1 name and URI
        getUtil().switchExecutor(INSTANCE_1);
        WikiReplicationAdministrationSectionPage admin1 = WikiReplicationAdministrationSectionPage.gotoPage();
        admin1.setCurrentName(INSTANCES[1].name);
        admin1.setCurrentURI(INSTANCES[1].proxyURI);
        admin1 = admin0.clickSaveButton();
        setCustom(INSTANCES[1].custom);
        assertEquals(INSTANCES[1].custom, getCustom(INSTANCES[1].proxyURI));
        // Configure instance2 name and URI
        getUtil().switchExecutor(INSTANCE_2);
        WikiReplicationAdministrationSectionPage admin2 = WikiReplicationAdministrationSectionPage.gotoPage();
        admin2.setCurrentName(INSTANCES[2].name);
        admin2.setCurrentURI(INSTANCES[2].proxyURI);
        admin2 = admin0.clickSaveButton();
        setCustom(INSTANCES[2].custom);
        assertEquals(INSTANCES[2].custom, getCustom(INSTANCES[2].proxyURI));

        // Go back to instance0
        getUtil().switchExecutor(INSTANCE_0);
        admin0 = WikiReplicationAdministrationSectionPage.gotoPage();
        // Link to instance1
        admin0.setRequestedURI(INSTANCES[1].proxyURI);
        admin0 = admin0.requestInstance();
        // Check if the instance has been added to requested instances
        List<RequestedInstancePane> requestedInstances = admin0.getRequestedInstances();
        assertEquals(1, requestedInstances.size());
        RequestedInstancePane requestedInstance = requestedInstances.get(0);
        assertEquals(INSTANCES[1].proxyURI, requestedInstance.getURI());
        String sendkey0To1 = requestedInstance.getSendKey();

        // Make sure the other cluster member have the changes
        getUtil().switchExecutor(INSTANCE_0_2);
        admin0 = WikiReplicationAdministrationSectionPage.gotoPage();
        // Check if the instance has been added to requested instances
        requestedInstances = admin0.getRequestedInstances();
        assertEquals(1, requestedInstances.size());
        requestedInstance = requestedInstances.get(0);
        assertEquals(INSTANCES[1].proxyURI, requestedInstance.getURI());
        assertEquals(sendkey0To1, requestedInstance.getSendKey());

        // Go to instance1
        getUtil().switchExecutor(INSTANCE_1);
        // Check if the instance has been added to requesting instances
        admin1 = assertEqualsRequestingInstancesWithTimeout(1);

        List<RequestingInstancePane> requestingInstances = admin1.getRequestingInstances();
        RequestingInstancePane requestingInstance = requestingInstances.get(0);
        assertEquals(INSTANCES[0].proxyURI, requestingInstance.getURI());
        assertEquals(INSTANCES[0].name, requestingInstance.getName());
        String receivekey0To1 = requestingInstance.getReceiveKey();

        assertEquals(sendkey0To1, receivekey0To1);

        // Accept the instance
        admin1 = requestingInstance.accept();

        // Check if the instance has been moved to registered instances
        List<RegisteredInstancePane> registeredInstances = admin1.getRegisteredInstances();
        assertEquals(1, registeredInstances.size());
        RegisteredInstancePane registeredInstance = registeredInstances.get(0);
        assertEquals(INSTANCES[0].proxyURI, registeredInstance.getURI());
        assertEquals(INSTANCES[0].name, registeredInstance.getName());
        receivekey0To1 = registeredInstance.getReceiveKey();
        String sendkey1To0 = registeredInstance.getSendKey();

        // Check if instance0 custom property was shared with instance1
        assertEqualsCustomWithTimeout(INSTANCES[0].proxyURI, INSTANCES[0].custom);

        // Link to instance2
        admin1.setRequestedURI(INSTANCES[2].proxyURI);
        admin1 = admin1.requestInstance();

        // Check if the instance has been moved to requesting instances
        requestingInstances = admin1.getRequestingInstances();
        assertEquals(0, requestingInstances.size());

        // Go back to instance0
        getUtil().switchExecutor(INSTANCE_0);
        // Check if the instance has been moved to registered instances
        admin0 = assertEqualsRequestedInstancesWithTimeout(0);

        // Check if the instance has been moved to registered instances
        registeredInstances = admin0.getRegisteredInstances();
        assertEquals(1, registeredInstances.size());
        registeredInstance = registeredInstances.get(0);
        assertEquals(INSTANCES[1].proxyURI, registeredInstance.getURI());
        assertEquals(INSTANCES[1].name, registeredInstance.getName());
        String receivekey1To0 = registeredInstance.getReceiveKey();
        assertEquals(sendkey1To0, receivekey1To0);

        // Check if instance1 custom property was shared with instance0
        assertEqualsCustomWithTimeout(INSTANCES[1].proxyURI, INSTANCES[1].custom);

        // Reset the send key to instance1
        admin0 = registeredInstance.resetKey();
        registeredInstances = admin0.getRegisteredInstances();
        registeredInstance = registeredInstances.get(0);
        sendkey0To1 = registeredInstance.getSendKey();

        // Make sure the other cluster member have the changes
        getUtil().switchExecutor(INSTANCE_0);
        // Check if the instance has been moved to registered instances
        admin0 = assertEqualsRequestedInstancesWithTimeout(0);
        registeredInstances = admin0.getRegisteredInstances();
        assertEquals(1, registeredInstances.size());
        registeredInstance = registeredInstances.get(0);
        assertEquals(INSTANCES[1].proxyURI, registeredInstance.getURI());
        assertEquals(INSTANCES[1].name, registeredInstance.getName());
        assertEquals(sendkey1To0, registeredInstance.getReceiveKey());

        // Go to instance1
        getUtil().switchExecutor(INSTANCE_1);
        // Make sure the receive key for instance0 was updated
        admin1 = WikiReplicationAdministrationSectionPage.gotoPage();
        registeredInstances = admin1.getRegisteredInstances();
        registeredInstance = registeredInstances.get(0);
        receivekey0To1 = registeredInstance.getReceiveKey();

        assertEquals(sendkey0To1, receivekey0To1);

        // Go to instance2
        getUtil().switchExecutor(INSTANCE_2);
        // Check if the instance has been added to requesting instances
        admin2 = assertEqualsRequestingInstancesWithTimeout(1);

        requestingInstances = admin2.getRequestingInstances();
        requestingInstance = requestingInstances.get(0);
        assertEquals(INSTANCES[1].proxyURI, requestingInstance.getURI());
        assertEquals(INSTANCES[1].name, requestingInstance.getName());

        // Accept the instance
        requestingInstance.accept();

        // Modify custom property on instance0
        getUtil().switchExecutor(INSTANCE_0);
        setCustom("modified0");

        // Make sure the custom property associated with instance0 was updated on instance1
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsCustomWithTimeout(INSTANCES[0].proxyURI, "modified0");

        // Make sure the custom property associated with instance0 was updated on instance2
        getUtil().switchExecutor(INSTANCE_2);
        assertEqualsCustomWithTimeout(INSTANCES[0].proxyURI, "modified0");
    }

    @Test
    public void all_main() throws Exception
    {
        // Set the main wiki
        getUtil().setCurrentWiki(MAIN_WIKI);

        // Execute tests
        all();
    }

    // @Test
    public void all_sub() throws Exception
    {
        // Set the sub wiki
        getUtil().setCurrentWiki(SUB_WIKI);

        // Execute tests
        all();
    }

    private void all() throws Exception
    {
        // go to home page
        getUtil().gotoPage(new LocalDocumentReference("Main", "WebHome"));

        // Configure replication
        controller();

        // 0 <-> 1 <-> 2
        replicate_0_ALL();

        // 0 -> 1 -> 2
        replicate_0_ALLSENDONLY();

        // 0 r-> 1 r-> 2
        replicate_0_REFERENCE();

        // 0 <-> 1 r-> 2
        replicate_1_0ALL_2REFERENCE();

        // 0 <-> 1 -> 2
        replicate_1_0ALL_2ALLSENDONLY();

        // 0 <-> / -> 1 -> 2
        replicate_1_0ALLRECEIVEONLYCONVERTONRECEIVE_2ALLSENDONLY();

        // Attachment replication
        replicateAttachments();

        // Replication reaction to configuration change
        controllerChanges();

        // Replication reliability
        network();

        // Replication conflict handling
        conflict();
    }

    private void controller() throws Exception
    {
        ////////////////////////
        // ALL replication

        // Configure ReplicationALL space replication on instance0
        getUtil().switchExecutor(INSTANCE_0);
        PageReplicationAdministrationSectionPage replicationPageAdmin =
            PageReplicationAdministrationSectionPage.gotoPage(REPLICATION_ALL_BOTH);
        replicationPageAdmin.setSpaceLevel(DocumentReplicationLevel.ALL);
        replicationPageAdmin.setSpaceDirection(DocumentReplicationDirection.BOTH);
        // Save replication configuration
        replicationPageAdmin.save();

        // Make sure the other cluster member have the changes
        if (INSTANCE_0_2_ENABLED) {
            getUtil().switchExecutor(INSTANCE_0_2);
            replicationPageAdmin = assertReplicationModeWithTimeout(REPLICATION_ALL_BOTH.getParent(), "all");
            assertSame(DocumentReplicationLevel.ALL, replicationPageAdmin.getSpaceLevel());
            assertSame(DocumentReplicationDirection.BOTH, replicationPageAdmin.getSpaceDirection());
        }

        // Make sure the configuration is replicated as expected on instance1
        getUtil().switchExecutor(INSTANCE_1);
        replicationPageAdmin = assertReplicationModeWithTimeout(REPLICATION_ALL_BOTH.getParent(), "all");
        assertSame(DocumentReplicationLevel.ALL, replicationPageAdmin.getSpaceLevel());
        assertSame(DocumentReplicationDirection.BOTH, replicationPageAdmin.getSpaceDirection());

        // Make sure the configuration is replicated as expected on instance2
        getUtil().switchExecutor(INSTANCE_2);
        replicationPageAdmin = assertReplicationModeWithTimeout(REPLICATION_ALL_BOTH.getParent(), "all");
        assertSame(DocumentReplicationLevel.ALL, replicationPageAdmin.getSpaceLevel());
        assertSame(DocumentReplicationDirection.BOTH, replicationPageAdmin.getSpaceDirection());

        ////////////////////////
        // ALL SENDONLY replication

        // Configure space replication on instance0
        getUtil().switchExecutor(INSTANCE_0);
        replicationPageAdmin = PageReplicationAdministrationSectionPage.gotoPage(REPLICATION_ALL_SEND_ONLY);
        replicationPageAdmin.setSpaceLevel(DocumentReplicationLevel.ALL);
        replicationPageAdmin.setSpaceDirection(DocumentReplicationDirection.SEND_ONLY);
        // Save replication configuration
        replicationPageAdmin.save();

        // Make sure the configuration is replicated as expected on instance1
        getUtil().switchExecutor(INSTANCE_1);
        replicationPageAdmin = assertReplicationModeWithTimeout(REPLICATION_ALL_SEND_ONLY.getParent(), "single");
        assertSame(DocumentReplicationLevel.ALL, replicationPageAdmin.getSpaceLevel(INSTANCES[0].proxyURI));
        assertSame(DocumentReplicationDirection.RECEIVE_ONLY,
            replicationPageAdmin.getSpaceDirection(INSTANCES[0].proxyURI));
        assertNull(replicationPageAdmin.getSpaceLevel(INSTANCES[2].proxyURI));

        // Make sure the configuration is replicated as expected on instance2
        getUtil().switchExecutor(INSTANCE_2);
        replicationPageAdmin = assertReplicationModeWithTimeout(REPLICATION_ALL_SEND_ONLY.getParent(), "all");
        assertSame(DocumentReplicationLevel.ALL, replicationPageAdmin.getSpaceLevel());
        assertSame(DocumentReplicationDirection.RECEIVE_ONLY, replicationPageAdmin.getSpaceDirection());

        ////////////////////////
        // REFERENCE replication

        // Configure ReplicationREFERENCE space replication on instance0
        getUtil().switchExecutor(INSTANCE_0);
        replicationPageAdmin = PageReplicationAdministrationSectionPage.gotoPage(REPLICATION_REFERENCE);
        replicationPageAdmin.setSpaceLevel(DocumentReplicationLevel.REFERENCE);
        // Save replication configuration
        replicationPageAdmin.save();
    }

    private void replicate_0_ALL() throws Exception
    {
        Page page = new Page();
        page.setSpace(REPLICATION_ALL_BOTH.getParent().getName());
        page.setName("ReplicatedPage");

        LocalDocumentReference documentReference = new LocalDocumentReference(page.getSpace(), page.getName());

        ////////////////////////////////////
        // Page creation on XWiki 0
        ////////////////////////////////////

        // Create a page on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        page.setContent("content");
        getUtil().rest().save(page);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, false);

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, false);

        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, false);

        ////////////////////////////////////
        // Minor edit on XWiki 0
        ////////////////////////////////////

        // Edit a page on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        page.setContent("minor content");
        saveMinor(page);
        assertDocumentWithTimeout(documentReference, "minor content", "1.2", 0, false);

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertDocumentWithTimeout(documentReference, "minor content", "1.2", 0, false);

        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentWithTimeout(documentReference, "minor content", "1.2", 0, false);

        ////////////////////////////////////
        // Major edit on XWiki 1
        ////////////////////////////////////

        // Modify content of the page on XWiki 1
        getUtil().switchExecutor(INSTANCE_1);
        page.setContent("modified content");
        getUtil().rest().save(page);
        assertDocumentWithTimeout(documentReference, "modified content", "2.1", 0, false);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_0);
        assertDocumentWithTimeout(documentReference, "modified content", "2.1", 0, false);

        // ASSERT) The content in XWiki 2 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentWithTimeout(documentReference, "modified content", "2.1", 0, false);

        ////////////////////////////////////
        // Major edit on XWiki 2
        ////////////////////////////////////

        // Modify content of the page on XWiki 2
        getUtil().switchExecutor(INSTANCE_2);
        page.setContent("modified content 2");
        getUtil().rest().save(page);
        assertDocumentWithTimeout(documentReference, "modified content 2", "3.1", 0, false);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 2
        getUtil().switchExecutor(INSTANCE_0);
        assertDocumentWithTimeout(documentReference, "modified content 2", "3.1", 0, false);

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 2
        getUtil().switchExecutor(INSTANCE_1);
        assertDocumentWithTimeout(documentReference, "modified content 2", "3.1", 0, false);

        ////////////////////////////////////
        // Delete version on XWiki 0
        ////////////////////////////////////

        // Delete a page history version on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        assertEqualsHistorySizeWithTimeout(documentReference, 4);
        History history = getHistory(documentReference);
        HistorySummary historySummary = history.getHistorySummaries().get(0);
        assertEquals("3.1", historySummary.getVersion());
        historySummary = history.getHistorySummaries().get(1);
        assertEquals("2.1", historySummary.getVersion());
        historySummary = history.getHistorySummaries().get(2);
        assertEquals("1.2", historySummary.getVersion());
        historySummary = history.getHistorySummaries().get(3);
        assertEquals("1.1", historySummary.getVersion());

        getUtil().recacheSecretToken();
        getUtil().deleteVersion(page.getSpace(), page.getName(), "1.2");

        assertEqualsHistorySizeWithTimeout(documentReference, 3);
        history = getHistory(documentReference);
        historySummary = history.getHistorySummaries().get(0);
        assertEquals("3.1", historySummary.getVersion());
        historySummary = history.getHistorySummaries().get(1);
        assertEquals("2.1", historySummary.getVersion());
        historySummary = history.getHistorySummaries().get(2);
        assertEquals("1.1", historySummary.getVersion());

        // ASSERT) The history in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsHistorySizeWithTimeout(documentReference, 3);
        history = getHistory(documentReference);
        historySummary = history.getHistorySummaries().get(0);
        assertEquals("3.1", historySummary.getVersion());
        historySummary = history.getHistorySummaries().get(1);
        assertEquals("2.1", historySummary.getVersion());
        historySummary = history.getHistorySummaries().get(2);
        assertEquals("1.1", historySummary.getVersion());

        // ASSERT) The history in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsHistorySizeWithTimeout(documentReference, 3);
        history = getHistory(documentReference);
        historySummary = history.getHistorySummaries().get(0);
        assertEquals("3.1", historySummary.getVersion());
        historySummary = history.getHistorySummaries().get(1);
        assertEquals("2.1", historySummary.getVersion());
        historySummary = history.getHistorySummaries().get(2);
        assertEquals("1.1", historySummary.getVersion());

        ////////////////////////////////////
        // Delete current version on XWiki 0
        ////////////////////////////////////

        // Delete a page history version on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        getUtil().gotoPage(page.getSpace(), page.getName());
        getUtil().recacheSecretToken();
        getUtil().deleteVersion(page.getSpace(), page.getName(), "3.1");

        assertEqualsHistorySizeWithTimeout(documentReference, 2);
        history = getHistory(documentReference);
        historySummary = history.getHistorySummaries().get(0);
        assertEquals("2.1", historySummary.getVersion());
        historySummary = history.getHistorySummaries().get(1);
        assertEquals("1.1", historySummary.getVersion());

        // ASSERT) The history in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsHistorySizeWithTimeout(documentReference, 2);
        assertEquals("modified content", getUtil().rest().<Page>get(documentReference).getContent());
        history = getHistory(documentReference);
        historySummary = history.getHistorySummaries().get(0);
        assertEquals("2.1", historySummary.getVersion());
        historySummary = history.getHistorySummaries().get(1);
        assertEquals("1.1", historySummary.getVersion());

        // ASSERT) The history in XWiki 2 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_2);
        assertEqualsHistorySizeWithTimeout(documentReference, 2);
        assertEquals("modified content", getUtil().rest().<Page>get(documentReference).getContent());
        history = getHistory(documentReference);
        historySummary = history.getHistorySummaries().get(0);
        assertEquals("2.1", historySummary.getVersion());
        historySummary = history.getHistorySummaries().get(1);
        assertEquals("1.1", historySummary.getVersion());

        ////////////////////////////////////
        // Delete document on XWiki 0
        ////////////////////////////////////

        getUtil().switchExecutor(INSTANCE_0);
        getUtil().rest().delete(documentReference);

        // TODO: ASSERT) Get the deleted document id

        // ASSERT) The document should not exist anymore on XWiki 1
        getUtil().switchExecutor(INSTANCE_1);
        // Since it can take time for the replication to propagate the change, we need to wait and set up a timeout.
        assertDoesNotExistWithTimeout(documentReference);

        // TODO: ASSERT) The deleted document has the expected id

        // ASSERT) The document should not exist anymore on XWiki 2
        getUtil().switchExecutor(INSTANCE_2);
        // Since it can take time for the replication to propagate the change, we need to wait and set up a timeout.
        assertDoesNotExistWithTimeout(documentReference);

        ////////////////////////////////////
        // Re-create the document in XWiki 2
        ////////////////////////////////////

        // Create a page on XWiki 2
        getUtil().switchExecutor(INSTANCE_2);
        page.setContent("content");
        getUtil().rest().save(page);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, false);

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 2
        getUtil().switchExecutor(INSTANCE_1);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, false);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 2
        getUtil().switchExecutor(INSTANCE_0);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, false);
    }

    private void replicate_0_ALLSENDONLY() throws Exception
    {
        ////////////////////////////////////
        // Replication
        ////////////////////////////////////

        Page page = new Page();
        page.setSpace(REPLICATION_ALL_SEND_ONLY.getParent().getName());
        page.setName("ReplicatedPage");

        LocalDocumentReference documentReference = new LocalDocumentReference(page.getSpace(), page.getName());

        ////////////////////////////////////
        // Page creation on XWiki 0
        ////////////////////////////////////

        // Create a page on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        page.setContent("content");
        getUtil().rest().save(page);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, false);

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, true);

        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, true);

        ////////////////////////////////////
        // Minor edit on XWiki 0
        ////////////////////////////////////

        // Edit a page on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        page.setContent("minor content");
        saveMinor(page);
        assertDocumentWithTimeout(documentReference, "minor content", "1.2", 0, false);

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertDocumentWithTimeout(documentReference, "minor content", "1.2", 0, true);

        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentWithTimeout(documentReference, "minor content", "1.2", 0, true);

        ////////////////////////////////////
        // Delete document on XWiki 0
        ////////////////////////////////////

        getUtil().switchExecutor(INSTANCE_0);
        getUtil().rest().delete(documentReference);

        // TODO: ASSERT) Get the deleted document id

        // ASSERT) The document should not exist anymore on XWiki 1
        getUtil().switchExecutor(INSTANCE_1);
        // Since it can take time for the replication to propagate the change, we need to wait and set up a timeout.
        assertDoesNotExistWithTimeout(documentReference);

        // TODO: ASSERT) The deleted document has the expected id

        // ASSERT) The document should not exist anymore on XWiki 2
        getUtil().switchExecutor(INSTANCE_2);
        // Since it can take time for the replication to propagate the change, we need to wait and set up a timeout.
        assertDoesNotExistWithTimeout(documentReference);
    }

    private void replicate_0_REFERENCE() throws Exception
    {
        Page page = new Page();
        page.setSpace(REPLICATION_REFERENCE.getParent().getName());
        page.setName("ReplicatedPage");

        LocalDocumentReference documentReference = new LocalDocumentReference(page.getSpace(), page.getName());

        ////////////////////////////////////
        // Page creation on XWiki 0
        ////////////////////////////////////

        // Create a page on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        page.setContent("content");
        getUtil().rest().save(page);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, false);

        // ASSERT) The page should exist but be empty on XWiki 1
        getUtil().switchExecutor(INSTANCE_1);
        assertDocumentREFERENCEWithTimeout(documentReference, 0);

        // ASSERT) The page should exist but be empty on XWiki 2
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentREFERENCEWithTimeout(documentReference, 0);

        ////////////////////////////////////
        // Edit on XWiki 0
        ////////////////////////////////////

        // Edit a page on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        page.setContent("modified content");
        getUtil().rest().save(page);
        assertDocumentWithTimeout(documentReference, "modified content", "2.1", 0, false);

        // We have to wait for a given time since we don't really have any criteria to test that something was not done,
        // let's hope 5s is enough for nothing to happen...
        Thread.sleep(5000);

        // ASSERT) That should not have any kind of impact on XWiki 1
        getUtil().switchExecutor(INSTANCE_1);
        assertDocumentREFERENCEWithTimeout(documentReference, 0);

        // ASSERT) The page should exist but be empty on XWiki 2
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentREFERENCEWithTimeout(documentReference, 0);

        ////////////////////////////////////
        // Edit on XWiki 1
        ////////////////////////////////////

        getUtil().switchExecutor(INSTANCE_1);
        page.setContent("forbidden modified content");
        getUtil().rest().save(page);
        // TODO: this should actually be forbidden (see https://jira.xwiki.org/browse/REPLICAT-113)
        assertEquals("forbidden modified content", getUtil().rest().<Page>get(documentReference).getContent());

        // We have to wait for a given time since we don't really have any criteria to test that something was not done,
        // let's hope 5s is enough for nothing to happen...
        Thread.sleep(5000);

        // ASSERT) That should not have any kind of impact on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        page = getUtil().rest().<Page>get(documentReference);
        assertEquals("Document was modified", "modified content", page.getContent());
        assertEquals("2.1", page.getVersion());

        ////////////////////////////////////
        // Delete document on XWiki 0
        ////////////////////////////////////

        getUtil().switchExecutor(INSTANCE_0);
        getUtil().rest().delete(documentReference);

        // TODO: ASSERT) Get the deleted document id

        // ASSERT) The document should not exist anymore on XWiki 1
        getUtil().switchExecutor(INSTANCE_1);
        // Since it can take time for the replication to propagate the change, we need to wait and set up a timeout.
        assertDoesNotExistWithTimeout(documentReference);

        // TODO: ASSERT) The deleted document has the expected id
    }

    private void replicate_1_0ALL_2REFERENCE() throws Exception
    {
        LocalDocumentReference documentReference =
            new LocalDocumentReference("Replication_1_0ALL_2REFERENCE", "WebHome");

        // Configure replication
        getUtil().switchExecutor(INSTANCE_1);
        setConfiguration(documentReference.getParent(), INSTANCES[0].proxyURI, DocumentReplicationLevel.ALL);
        setConfiguration(documentReference.getParent(), INSTANCES[2].proxyURI, DocumentReplicationLevel.REFERENCE);

        Page page = new Page();
        page.setSpace(documentReference.getParent().getName());
        page.setName(documentReference.getName());

        ////////////////////////////////////
        // Page creation on XWiki 1
        ////////////////////////////////////

        // Create a page on XWiki 1
        getUtil().switchExecutor(INSTANCE_1);
        page.setContent("content");
        getUtil().rest().save(page);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 1, false);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_0);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 1, false);

        // ASSERT) The page should exist but be empty on XWiki 2
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentREFERENCEWithTimeout(documentReference, 1);
    }

    private void replicate_1_0ALL_2ALLSENDONLY() throws Exception
    {
        LocalDocumentReference documentReference =
            new LocalDocumentReference("Replication_1_0ALL_2ALLSENDONLY", "WebHome");

        // Configure replication
        getUtil().switchExecutor(INSTANCE_1);
        setConfiguration(documentReference.getParent(), INSTANCES[0].proxyURI, DocumentReplicationLevel.ALL);
        setConfiguration(documentReference.getParent(), INSTANCES[2].proxyURI, DocumentReplicationLevel.ALL, true);

        Page page = new Page();
        page.setSpace(documentReference.getParent().getName());
        page.setName(documentReference.getName());

        ////////////////////////////////////
        // Page creation on XWiki 0
        ////////////////////////////////////

        // Create a page on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        page.setContent("content");
        getUtil().rest().save(page);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, false);

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, false);

        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0 but readonly
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, true);

        ////////////////////////////////////
        // Minor edit on XWiki 0
        ////////////////////////////////////

        // Edit a page on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        page.setContent("modified content");
        getUtil().rest().save(page);
        assertDocumentWithTimeout(documentReference, "modified content", "2.1", 0, false);

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertDocumentWithTimeout(documentReference, "modified content", "2.1", 0, false);

        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0 but readonly
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentWithTimeout(documentReference, "modified content", "2.1", 0, true);
    }

    private void replicate_1_0ALLRECEIVEONLYCONVERTONRECEIVE_2ALLSENDONLY() throws Exception
    {
        LocalDocumentReference documentReference =
            new LocalDocumentReference("replicate_1_0ALLRECEIVEONLYCONVERTONRECEIVE_2ALLSENDONLY", "WebHome");

        // Configure replication
        getUtil().switchExecutor(INSTANCE_1);
        setConfiguration(documentReference.getParent(), INSTANCES[0].proxyURI, DocumentReplicationLevel.ALL);
        setConfiguration(documentReference.getParent(), INSTANCES[2].proxyURI, DocumentReplicationLevel.ALL, true);

        // Make sure XWiki 1 convert received messages to readonly
        setConfigurationLocal(documentReference.getParent(), INSTANCES[0].proxyURI, DocumentReplicationLevel.ALL,
            DocumentReplicationDirection.RECEIVE_ONLY);

        Page page = new Page();
        page.setSpace(documentReference.getParent().getName());
        page.setName(documentReference.getName());

        ////////////////////////////////////
        // Page creation on XWiki 0
        ////////////////////////////////////

        // Create a page on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        page.setContent("content");
        getUtil().rest().save(page);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, false);

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, true);

        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0 but readonly
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentWithTimeout(documentReference, "content", "1.1", 0, true);

        ////////////////////////////////////
        // Page modification on XWiki 0
        ////////////////////////////////////

        // Edit a page on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        page.setContent("modified content");
        getUtil().rest().save(page);
        assertDocumentWithTimeout(documentReference, "modified content", "2.1", 0, false);

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertDocumentWithTimeout(documentReference, "modified content", "2.1", 0, true);

        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0 but readonly
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentWithTimeout(documentReference, "modified content", "2.1", 0, true);
    }

    private void replicateAttachments() throws Exception
    {
        Page page = new Page();
        page.setSpace("ReplicatedWithAttachments");
        page.setName("WebHome");

        LocalDocumentReference documentReference = new LocalDocumentReference(page.getSpace(), page.getName());
        EntityReference attachment1Reference =
            new EntityReference("attach1.txt", EntityType.ATTACHMENT, documentReference);
        EntityReference attachment2Reference =
            new EntityReference("attach2.txt", EntityType.ATTACHMENT, documentReference);

        //////////////////////
        // Replicate a new page with attachment

        // Create a page on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        page.setContent("content");
        getUtil().rest().save(page);
        // Add an attachment to the page
        getUtil().attachFile(attachment1Reference, new ByteArrayInputStream("attach1".getBytes()), true);
        assertEquals("attach1", getAttachmentContent(attachment1Reference));

        // Enabled replication
        setConfiguration(documentReference, DocumentReplicationLevel.ALL);

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsVersionWithTimeout(documentReference, "2.1");
        assertEquals("attach1", getAttachmentContent(attachment1Reference));
        page = getPageWithAttachments(documentReference);
        Attachment attachment1 = getAttachment(page, attachment1Reference.getName());
        assertEquals("1.1", attachment1.getVersion());

        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertEqualsVersionWithTimeout(documentReference, "2.1");
        assertEquals("attach1", getAttachmentContent(attachment1Reference));
        page = getPageWithAttachments(documentReference);
        attachment1 = getAttachment(page, attachment1Reference.getName());
        assertEquals("1.1", attachment1.getVersion());

        //////////////////////
        // Add attachments

        // Add another attachment on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        getUtil().attachFile(attachment2Reference, new ByteArrayInputStream("attach2".getBytes()), true);
        assertEquals("attach2", getAttachmentContent(attachment2Reference));

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsVersionWithTimeout(documentReference, "3.1");
        assertEquals("attach1", getAttachmentContent(attachment1Reference));
        assertEquals("attach2", getAttachmentContent(attachment2Reference));
        page = getPageWithAttachments(documentReference);
        attachment1 = getAttachment(page, attachment1Reference.getName());
        assertEquals("1.1", attachment1.getVersion());
        Attachment attachment2 = getAttachment(page, attachment2Reference.getName());
        assertEquals("1.1", attachment2.getVersion());

        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertEqualsVersionWithTimeout(documentReference, "3.1");
        assertEquals("attach1", getAttachmentContent(attachment1Reference));
        assertEquals("attach2", getAttachmentContent(attachment2Reference));
        page = getPageWithAttachments(documentReference);
        attachment1 = getAttachment(page, attachment1Reference.getName());
        assertEquals("1.1", attachment1.getVersion());
        attachment2 = getAttachment(page, attachment2Reference.getName());
        assertEquals("1.1", attachment2.getVersion());

        //////////////////////
        // Update existing attachment

        // Update attachment1 on XWiki 0
        getUtil().switchExecutor(INSTANCE_0);
        getUtil().attachFile(attachment1Reference, new ByteArrayInputStream("attach1modified".getBytes()), false);
        assertEquals("attach1modified", getAttachmentContent(attachment1Reference));
        assertEquals("attach2", getAttachmentContent(attachment2Reference));

        // ASSERT) The content in XWiki 1 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsVersionWithTimeout(documentReference, "4.1");
        assertEquals("attach1modified", getAttachmentContent(attachment1Reference));
        assertEquals("attach2", getAttachmentContent(attachment2Reference));
        page = getPageWithAttachments(documentReference);
        attachment1 = getAttachment(page, attachment1Reference.getName());
        assertEquals("1.2", attachment1.getVersion());
        attachment2 = getAttachment(page, attachment2Reference.getName());
        assertEquals("1.1", attachment2.getVersion());

        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertEqualsVersionWithTimeout(documentReference, "4.1");
        assertEquals("attach1modified", getAttachmentContent(attachment1Reference));
        assertEquals("attach2", getAttachmentContent(attachment2Reference));
        page = getPageWithAttachments(documentReference);
        attachment1 = getAttachment(page, attachment1Reference.getName());
        assertEquals("1.2", attachment1.getVersion());
        attachment2 = getAttachment(page, attachment2Reference.getName());
        assertEquals("1.1", attachment2.getVersion());
    }

    private Attachment getAttachment(Page page, String name)
    {
        for (Attachment attachment : page.getAttachments().getAttachments()) {
            if (attachment.getName().equals(name)) {
                return attachment;
            }
        }

        return null;
    }

    private void setConfiguration(EntityReference reference, DocumentReplicationLevel level)
    {
        setConfiguration(reference, null, level);
    }

    private void setConfiguration(EntityReference reference, String instance, DocumentReplicationLevel level)
    {
        setConfiguration(reference, instance, level, false);
    }

    private void setConfiguration(EntityReference reference, String instance, DocumentReplicationLevel level,
        boolean readonly)
    {
        PageReplicationAdministrationSectionPage replicationPageAdmin =
            PageReplicationAdministrationSectionPage.gotoPage(reference);

        if (reference.getType() == EntityType.SPACE) {
            replicationPageAdmin.setSpaceLevel(instance, level);
            if (readonly) {
                replicationPageAdmin.setSpaceDirection(instance, DocumentReplicationDirection.SEND_ONLY);
            }
        } else {
            replicationPageAdmin.setDocumentLevel(instance, level);
            if (readonly) {
                replicationPageAdmin.setDocumentDirection(instance, DocumentReplicationDirection.SEND_ONLY);
            }
        }

        replicationPageAdmin.save();
    }

    private void setConfigurationLocal(EntityReference reference, String instance, DocumentReplicationLevel level,
        DocumentReplicationDirection direction) throws Exception
    {
        String body = getUtil().executeAndGetBodyAsString(SERVICE_SETCONFIGURATION_REFERENCE,
            Map.of("outputSyntax", "plain", "reference",
                reference.getType().name().toLowerCase() + ':' + getUtil().serializeReference(reference), "instanceURI",
                instance, "instanceLevel", level.name(), "instanceDirection", direction.name()));

        System.out.println("setConfigurationLocal: " + body);
    }

    private void controllerChanges() throws Exception
    {
        EntityReference page1Space = new EntityReference("page1", EntityType.SPACE);
        LocalDocumentReference page1 = new LocalDocumentReference("WebHome", page1Space);
        EntityReference page1_1Space = new EntityReference("page1_1", EntityType.SPACE, page1Space);
        LocalDocumentReference page1_1 = new LocalDocumentReference("WebHome", page1_1Space);
        LocalDocumentReference page1_1_1 =
            new LocalDocumentReference("WebHome", new EntityReference("page1_1_1", EntityType.SPACE, page1_1Space));
        LocalDocumentReference page1_2 =
            new LocalDocumentReference("WebHome", new EntityReference("page1_2", EntityType.SPACE, page1Space));

        getUtil().switchExecutor(INSTANCE_1);

        getUtil().rest().savePage(page1, "content1", "");
        getUtil().rest().savePage(page1_1, "content1_1", "");
        getUtil().rest().savePage(page1_1_1, "content1_1_1", "");
        getUtil().rest().savePage(page1_2, "content1_2", "");

        ////////////////////////////////////
        // Start ALL replication of document "page1.page1_1.WebHome"
        ////////////////////////////////////

        // Set replication configuration
        getUtil().switchExecutor(INSTANCE_1);
        setConfiguration(page1_1, DocumentReplicationLevel.ALL);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_0);
        assertEqualsContentWithTimeout(page1_1, "content1_1");
        assertDoesNotExistWithTimeout(page1_1_1);
        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertEqualsContentWithTimeout(page1_1, "content1_1");
        assertDoesNotExistWithTimeout(page1_1_1);

        ////////////////////////////////////
        // Switch to REFERENCE replication of document "page1.page1_1.WebHome"
        ////////////////////////////////////

        // Set replication configuration
        getUtil().switchExecutor(INSTANCE_1);
        setConfiguration(page1_1, DocumentReplicationLevel.REFERENCE);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_0);
        assertDocumentREFERENCEWithTimeout(page1_1, 1);
        assertDoesNotExistWithTimeout(page1_1_1);
        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentREFERENCEWithTimeout(page1_1, 1);
        assertDoesNotExistWithTimeout(page1_1_1);

        ////////////////////////////////////
        // Stop replication of document "page1.page1_1.WebHome"
        ////////////////////////////////////

        // Set replication configuration
        getUtil().switchExecutor(INSTANCE_1);
        setConfiguration(page1_1, null);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_0);
        assertDoesNotExistWithTimeout(page1_1);
        assertDoesNotExistWithTimeout(page1_1_1);
        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertDoesNotExistWithTimeout(page1_1);
        assertDoesNotExistWithTimeout(page1_1_1);

        ////////////////////////////////////
        // Start ALL replication of space "page1"
        ////////////////////////////////////

        // Set replication configuration
        getUtil().switchExecutor(INSTANCE_1);
        setConfiguration(page1Space, DocumentReplicationLevel.ALL);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_0);
        assertEqualsContentWithTimeout(page1, "content1");
        assertDoesNotExistWithTimeout(page1_1);
        assertEqualsContentWithTimeout(page1_1_1, "content1_1_1");
        assertEqualsContentWithTimeout(page1_2, "content1_2");
        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertEqualsContentWithTimeout(page1, "content1");
        assertDoesNotExistWithTimeout(page1_1);
        assertEqualsContentWithTimeout(page1_1_1, "content1_1_1");
        assertEqualsContentWithTimeout(page1_2, "content1_2");

        ////////////////////////////////////
        // Switch to REFERENCE replication of space "page1"
        ////////////////////////////////////

        // Set replication configuration
        getUtil().switchExecutor(INSTANCE_1);
        setConfiguration(page1Space, DocumentReplicationLevel.REFERENCE);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_0);
        assertDocumentREFERENCEWithTimeout(page1, 1);
        assertDoesNotExistWithTimeout(page1_1);
        assertDocumentREFERENCEWithTimeout(page1_1_1, 1);
        assertDocumentREFERENCEWithTimeout(page1_2, 1);
        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertDocumentREFERENCEWithTimeout(page1, 1);
        assertDoesNotExistWithTimeout(page1_1);
        assertDocumentREFERENCEWithTimeout(page1_1_1, 1);
        assertDocumentREFERENCEWithTimeout(page1_2, 1);

        ////////////////////////////////////
        // Stop replication of space "page1"
        ////////////////////////////////////

        // Set replication configuration
        getUtil().switchExecutor(INSTANCE_1);
        setConfiguration(page1Space, null);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_0);
        assertDoesNotExistWithTimeout(page1);
        assertDoesNotExistWithTimeout(page1_1);
        assertDoesNotExistWithTimeout(page1_1_1);
        assertDoesNotExistWithTimeout(page1_2);
        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertDoesNotExistWithTimeout(page1);
        assertDoesNotExistWithTimeout(page1_1);
        assertDoesNotExistWithTimeout(page1_1_1);
        assertDoesNotExistWithTimeout(page1_2);

        ////////////////////////////////////
        // Start ALL replication of space "page1" with XWIKI 0
        ////////////////////////////////////

        // Set replication configuration
        getUtil().switchExecutor(INSTANCE_1);
        setConfiguration(page1Space, INSTANCES[0].proxyURI, DocumentReplicationLevel.ALL);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_0);
        assertEqualsContentWithTimeout(page1, "content1");
        assertDoesNotExistWithTimeout(page1_1);
        assertEqualsContentWithTimeout(page1_1_1, "content1_1_1");
        assertEqualsContentWithTimeout(page1_2, "content1_2");
        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertDoesNotExistWithTimeout(page1);
        assertDoesNotExistWithTimeout(page1_1);
        assertDoesNotExistWithTimeout(page1_1_1);
        assertDoesNotExistWithTimeout(page1_2);

        ////////////////////////////////////
        // Start ALL replication of space "page1" with XWIKI 2
        ////////////////////////////////////

        getUtil().switchExecutor(INSTANCE_1);
        setConfiguration(page1Space, INSTANCES[2].proxyURI, DocumentReplicationLevel.ALL);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_0);
        assertEqualsContentWithTimeout(page1, "content1");
        assertDoesNotExistWithTimeout(page1_1);
        assertEqualsContentWithTimeout(page1_1_1, "content1_1_1");
        assertEqualsContentWithTimeout(page1_2, "content1_2");
        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertEqualsContentWithTimeout(page1, "content1");
        assertDoesNotExistWithTimeout(page1_1);
        assertEqualsContentWithTimeout(page1_1_1, "content1_1_1");
        assertEqualsContentWithTimeout(page1_2, "content1_2");

        ////////////////////////////////////
        // Stop replication of page1.page1 space on XWIKI 0
        ////////////////////////////////////

        getUtil().switchExecutor(INSTANCE_1);
        setConfiguration(page1Space, INSTANCES[0].proxyURI, null);

        // ASSERT) The content in XWiki 0 should be the one set in XWiki 1
        getUtil().switchExecutor(INSTANCE_0);
        assertDoesNotExistWithTimeout(page1);
        assertDoesNotExistWithTimeout(page1_1);
        assertDoesNotExistWithTimeout(page1_1_1);
        assertDoesNotExistWithTimeout(page1_2);
        // ASSERT) The content in XWiki 2 should be the one set in XWiki 0
        getUtil().switchExecutor(INSTANCE_2);
        assertEqualsContentWithTimeout(page1, "content1");
        assertDoesNotExistWithTimeout(page1_1);
        assertEqualsContentWithTimeout(page1_1_1, "content1_1_1");
        assertEqualsContentWithTimeout(page1_2, "content1_2");
    }

    private void like() throws Exception
    {
        LocalDocumentReference reference = new LocalDocumentReference("Like", "WebHome");

        // Configure replication
        setConfiguration(reference, DocumentReplicationLevel.ALL);
        // Make sure to wait until the configuration is replicated
        getUtil().switchExecutor(INSTANCE_1);
        assertReplicationModeWithTimeout(reference, "all");

        // Create a new page on XWIKI 0
        getUtil().switchExecutor(INSTANCE_0);
        getUtil().rest().savePage(reference, "content", "");
        getUtil().rest().<Page>get(reference);

        // Make sure the page replicated on XWIKI 1
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsContentWithTimeout(reference, "content");
        getUtil().rest().<Page>get(reference);

        // Set like on XWIKI 0
        getUtil().switchExecutor(INSTANCE_0);
        getUtil().gotoPage(reference);
        LikeButton like = new LikeButton();
        like.clickToLike();
        assertEqualsLikeWithTimeout(reference, 1);

        // Make sure the like is replicated
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsLikeWithTimeout(reference, 1);
    }

    private void network() throws Exception
    {
        LocalDocumentReference documentReference = new LocalDocumentReference("Network", "WebHome");

        // Configure replication
        getUtil().switchExecutor(INSTANCE_0);
        setConfiguration(documentReference, DocumentReplicationLevel.ALL);
        // Make sure to wait until the configuration is replicated
        getUtil().switchExecutor(INSTANCE_1);
        assertReplicationModeWithTimeout(documentReference, "all");

        // Check creation replication with delay
        replicateWithDelay(documentReference, "content", true);

        // Check update replication with delay
        replicateWithDelay(documentReference, "modified content", false);
    }

    private void replicateWithDelay(LocalDocumentReference documentReference, String content, boolean create)
        throws Exception
    {
        // Block the proxy
        proxy1.removeStub(INSTANCES[1].proxyStubBuilder);
        proxy1.stubFor(proxyFailingBuilder);

        // Create a new page on XWIKI 0
        getUtil().switchExecutor(INSTANCE_0);
        getUtil().rest().savePage(documentReference, content, "");
        Page page0 = getUtil().rest().<Page>get(documentReference);

        // We have to wait for a given time since we don't really have any criteria to test that something was not done,
        // let's hope 5s is enough for nothing to happen...
        Thread.sleep(5000);

        // Make sure the page is not replicated on XWIKI 1
        getUtil().switchExecutor(INSTANCE_1);
        if (create) {
            assertDoesNotExistWithTimeout(documentReference);
        } else {
            assertNotEquals(getUtil().rest().<Page>get(documentReference).getContent(), content);
        }

        // Switch to main wiki
        String currentWiki = getUtil().getCurrentWiki();
        getUtil().setCurrentWiki("xwiki");
        // Make sure the UI indicate this waiting message
        getUtil().switchExecutor(INSTANCE_0);
        WikiReplicationAdministrationSectionPage admin0 = WikiReplicationAdministrationSectionPage.gotoPage();
        List<RegisteredInstancePane> instances = admin0.getRegisteredInstances();
        RegisteredInstancePane instance = instances.get(0);
        assertEquals("1 messages still waiting to be sent to the replication instance", instance.getWarning());

        // Make sure that at there is at least 1s between the save and the replication to have an impact on the stored
        // date
        Thread.sleep(1000);

        // Proxy is back
        proxy1.removeStub(proxyFailingBuilder);
        proxy1.stubFor(INSTANCES[1].proxyStubBuilder);

        // Force pushing waiting messages
        instance.clickRetry();

        // Switch back to the wiki
        getUtil().setCurrentWiki(currentWiki);

        // Make sure the document is finally replicated on XWIKI 1
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsContentWithTimeout(documentReference, content);
        Page page1 = getUtil().rest().<Page>get(documentReference);

        // Make sure the initial date is kept
        assertEquals(page1.getModified(), page0.getModified());
    }

    private void conflict() throws Exception
    {
        LocalDocumentReference documentReference = new LocalDocumentReference("Conflict", "WebHome");

        // Configure replication
        setConfiguration(documentReference, DocumentReplicationLevel.ALL);
        // Make sure to wait until the configuration is replicated
        getUtil().switchExecutor(INSTANCE_1);
        assertReplicationModeWithTimeout(documentReference, "all");

        // Create a new page on XWIKI 0
        getUtil().switchExecutor(INSTANCE_0);
        getUtil().rest().savePage(documentReference, "content", "");
        assertEqualsHistorySizeWithTimeout(documentReference, 1);

        // Make sure the page is replicated
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsContentWithTimeout(documentReference, "content");
        assertEqualsHistorySizeWithTimeout(documentReference, 1);

        // Block network 0 <-> 1
        proxy0.removeStub(INSTANCES[0].proxyStubBuilder);
        proxy1.removeStub(INSTANCES[1].proxyStubBuilder);
        proxy0.stubFor(proxyFailingBuilder);
        proxy1.stubFor(proxyFailingBuilder);

        // Modify the page in XWIKI 0
        getUtil().switchExecutor(INSTANCE_0);
        getUtil().rest().savePage(documentReference, "content0", "");

        // Modify the page in XWIKI 1
        getUtil().switchExecutor(INSTANCE_1);
        getUtil().rest().savePage(documentReference, "content1", "");

        // Make sure that at there is at least 1 second between the save and the replication to have an impact on the
        // stored date
        Thread.sleep(1000);

        // Make sure the message are blocked
        getUtil().switchExecutor(INSTANCE_0);
        assertEqualsHistorySizeWithTimeout(documentReference, 2);
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsHistorySizeWithTimeout(documentReference, 2);

        // Enabled back network
        proxy0.removeStub(proxyFailingBuilder);
        proxy1.removeStub(proxyFailingBuilder);
        proxy0.stubFor(INSTANCES[0].proxyStubBuilder);
        proxy1.stubFor(INSTANCES[1].proxyStubBuilder);

        // Switch to main wiki
        String currentWiki = getUtil().getCurrentWiki();
        getUtil().setCurrentWiki("xwiki");
        // Force pushing waiting messages on XWIKI 0
        getUtil().switchExecutor(INSTANCE_0);
        WikiReplicationAdministrationSectionPage.gotoPage().getRegisteredInstances().get(0).clickRetry();
        // Switch back to the wiki
        getUtil().setCurrentWiki(currentWiki);

        // Wait for the conflict resolution
        getUtil().switchExecutor(INSTANCE_0);
        assertEqualsHistorySizeWithTimeout(documentReference, 4);
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsHistorySizeWithTimeout(documentReference, 4);

        // Check the result of the conflict resolution is the same on both instances
        getUtil().switchExecutor(INSTANCE_0);
        assertEqualsContentWithTimeout(documentReference, "content1");
        getUtil().switchExecutor(INSTANCE_1);
        assertEqualsContentWithTimeout(documentReference, "content1");

        // Make sure a warning is shown in case of conflict
        getUtil().switchExecutor(INSTANCE_0);
        ReplicationPage page0 = ReplicationPage.gotoPage(documentReference);
        ReplicationConflictPane conflict0 = page0.getReplicationConflictPane();
        assertNotNull(conflict0);
        getUtil().switchExecutor(INSTANCE_1);
        ReplicationPage page1 = ReplicationPage.gotoPage(documentReference);
        ReplicationConflictPane conflict1 = page1.getReplicationConflictPane();
        assertNotNull(conflict1);
    }
}
