/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index;

import lombok.SneakyThrows;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.env.Environment;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.jni.PlatformUtils;
import org.opensearch.knn.plugin.KNNPlugin;
import org.opensearch.node.MockNode;
import org.opensearch.node.Node;
import org.opensearch.Version;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.MockHttpTransport;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.test.NodeRoles.dataNode;

public class KNNSettingsTests extends KNNTestCase {

    private static final String INDEX_NAME = "myindex";

    @SneakyThrows
    public void testGetSettingValueFromConfig() {
        long expectedKNNCircuitBreakerLimit = 13;
        Node mockNode = createMockNode(
            Map.of(KNNSettings.KNN_MEMORY_CIRCUIT_BREAKER_CLUSTER_LIMIT, "\"" + expectedKNNCircuitBreakerLimit + "kb\"")
        );
        mockNode.start();
        ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
        KNNSettings.state().setClusterService(clusterService);
        long actualKNNCircuitBreakerLimit = ((ByteSizeValue) KNNSettings.state()
            .getSettingValue(KNNSettings.KNN_MEMORY_CIRCUIT_BREAKER_CLUSTER_LIMIT)).getKb();
        mockNode.close();
        assertEquals(expectedKNNCircuitBreakerLimit, actualKNNCircuitBreakerLimit);
    }

    @SneakyThrows
    public void testGetSettingValueDefault() {
        Node mockNode = createMockNode(Collections.emptyMap());
        mockNode.start();
        ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
        KNNSettings.state().setClusterService(clusterService);
        long actualKNNCircuitBreakerLimit = ((ByteSizeValue) KNNSettings.state()
            .getSettingValue(KNNSettings.KNN_MEMORY_CIRCUIT_BREAKER_CLUSTER_LIMIT)).getKb();
        mockNode.close();
        assertEquals(
            ((ByteSizeValue) KNNSettings.dynamicCacheSettings.get(KNNSettings.KNN_MEMORY_CIRCUIT_BREAKER_CLUSTER_LIMIT)
                .getDefault(Settings.EMPTY)).getKb(),
            actualKNNCircuitBreakerLimit

        );
    }

    @SneakyThrows
    public void testFilteredSearchAdvanceSetting_whenNoValuesProvidedByUsers_thenDefaultSettingsUsed() {
        Node mockNode = createMockNode(Collections.emptyMap());
        mockNode.start();
        ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
        mockNode.client().admin().cluster().state(new ClusterStateRequest()).actionGet();
        mockNode.client().admin().indices().create(new CreateIndexRequest(INDEX_NAME)).actionGet();
        KNNSettings.state().setClusterService(clusterService);

        Integer filteredSearchThreshold = KNNSettings.getFilteredExactSearchThreshold(INDEX_NAME);
        mockNode.close();
        assertEquals(KNNSettings.ADVANCED_FILTERED_EXACT_SEARCH_THRESHOLD_DEFAULT_VALUE, filteredSearchThreshold);
    }

    @SneakyThrows
    public void testFilteredSearchAdvanceSetting_whenValuesProvidedByUsers_thenValidateSameValues() {
        int userDefinedThreshold = 1000;
        int userDefinedThresholdMinValue = 0;
        Node mockNode = createMockNode(Collections.emptyMap());
        mockNode.start();
        ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
        mockNode.client().admin().cluster().state(new ClusterStateRequest()).actionGet();
        mockNode.client().admin().indices().create(new CreateIndexRequest(INDEX_NAME)).actionGet();
        KNNSettings.state().setClusterService(clusterService);

        final Settings filteredSearchAdvanceSettings = Settings.builder()
            .put(KNNSettings.ADVANCED_FILTERED_EXACT_SEARCH_THRESHOLD, userDefinedThreshold)
            .build();

        mockNode.client()
            .admin()
            .indices()
            .updateSettings(new UpdateSettingsRequest(filteredSearchAdvanceSettings, INDEX_NAME))
            .actionGet();

        int filteredSearchThreshold = KNNSettings.getFilteredExactSearchThreshold(INDEX_NAME);

        // validate if we are able to set MinValues for the setting
        final Settings filteredSearchAdvanceSettingsWithMinValues = Settings.builder()
            .put(KNNSettings.ADVANCED_FILTERED_EXACT_SEARCH_THRESHOLD, userDefinedThresholdMinValue)
            .build();

        mockNode.client()
            .admin()
            .indices()
            .updateSettings(new UpdateSettingsRequest(filteredSearchAdvanceSettingsWithMinValues, INDEX_NAME))
            .actionGet();

        int filteredSearchThresholdMinValue = KNNSettings.getFilteredExactSearchThreshold(INDEX_NAME);

        mockNode.close();
        assertEquals(userDefinedThreshold, filteredSearchThreshold);
        assertEquals(userDefinedThresholdMinValue, filteredSearchThresholdMinValue);
    }

    @SneakyThrows
    public void testGetEfSearch_whenNoValuesProvidedByUsers_thenDefaultSettingsUsed() {
        Node mockNode = createMockNode(Collections.emptyMap());
        mockNode.start();
        ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
        mockNode.client().admin().cluster().state(new ClusterStateRequest()).actionGet();
        mockNode.client().admin().indices().create(new CreateIndexRequest(INDEX_NAME)).actionGet();
        KNNSettings.state().setClusterService(clusterService);

        Integer efSearchValue = KNNSettings.getEfSearchParam(INDEX_NAME);
        mockNode.close();
        assertEquals(KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_EF_SEARCH, efSearchValue);
    }

    @SneakyThrows
    public void testGetEfSearch_whenEFSearchValueSetByUser_thenReturnValue() {
        int userProvidedEfSearch = 300;
        Node mockNode = createMockNode(Collections.emptyMap());
        mockNode.start();
        ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
        mockNode.client().admin().cluster().state(new ClusterStateRequest()).actionGet();
        final Settings settings = Settings.builder()
            .put(KNNSettings.KNN_ALGO_PARAM_EF_SEARCH, userProvidedEfSearch)
            .put(KNNSettings.KNN_INDEX, true)
            .build();
        mockNode.client().admin().indices().create(new CreateIndexRequest(INDEX_NAME, settings)).actionGet();
        KNNSettings.state().setClusterService(clusterService);

        int efSearchValue = KNNSettings.getEfSearchParam(INDEX_NAME);
        mockNode.close();
        assertEquals(userProvidedEfSearch, efSearchValue);
    }

    @SneakyThrows
    public void testShardLevelRescoringDisabled_whenNoValuesProvidedByUser_thenDefaultSettingsUsed() {
        Node mockNode = createMockNode(Collections.emptyMap());
        mockNode.start();
        ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
        mockNode.client().admin().cluster().state(new ClusterStateRequest()).actionGet();
        mockNode.client().admin().indices().create(new CreateIndexRequest(INDEX_NAME)).actionGet();
        KNNSettings.state().setClusterService(clusterService);

        boolean shardLevelRescoringDisabled = KNNSettings.isShardLevelRescoringDisabledForDiskBasedVector(INDEX_NAME);
        mockNode.close();
        assertFalse(shardLevelRescoringDisabled);
    }

    @SneakyThrows
    public void testShardLevelRescoringDisabled_whenValueProvidedByUser_thenSettingApplied() {
        boolean userDefinedRescoringDisabled = true;
        Node mockNode = createMockNode(Collections.emptyMap());
        mockNode.start();
        ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
        mockNode.client().admin().cluster().state(new ClusterStateRequest()).actionGet();
        mockNode.client().admin().indices().create(new CreateIndexRequest(INDEX_NAME)).actionGet();
        KNNSettings.state().setClusterService(clusterService);

        final Settings rescoringDisabledSetting = Settings.builder()
            .put(KNNSettings.KNN_DISK_VECTOR_SHARD_LEVEL_RESCORING_DISABLED, userDefinedRescoringDisabled)
            .build();

        mockNode.client().admin().indices().updateSettings(new UpdateSettingsRequest(rescoringDisabledSetting, INDEX_NAME)).actionGet();

        boolean shardLevelRescoringDisabled = KNNSettings.isShardLevelRescoringDisabledForDiskBasedVector(INDEX_NAME);
        mockNode.close();
        assertEquals(userDefinedRescoringDisabled, shardLevelRescoringDisabled);
    }

    @SneakyThrows
    public void testGetFaissAVX2DisabledSettingValueFromConfig_enableSetting_thenValidateAndSucceed() {
        boolean expectedKNNFaissAVX2Disabled = true;
        Node mockNode = createMockNode(Map.of(KNNSettings.KNN_FAISS_AVX2_DISABLED, expectedKNNFaissAVX2Disabled));
        mockNode.start();
        ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
        KNNSettings.state().setClusterService(clusterService);
        boolean actualKNNFaissAVX2Disabled = KNNSettings.state().getSettingValue(KNNSettings.KNN_FAISS_AVX2_DISABLED);
        mockNode.close();
        assertEquals(expectedKNNFaissAVX2Disabled, actualKNNFaissAVX2Disabled);
    }

    @SneakyThrows
    public void testGetIndexThreadQty_WithDifferentValues_thenSuccess() {
        Node mockNode = createMockNode(Map.of(KNNSettings.KNN_ALGO_PARAM_INDEX_THREAD_QTY, 3));
        mockNode.start();
        ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
        KNNSettings.state().setClusterService(clusterService);
        int threadQty = KNNSettings.getIndexThreadQty();
        mockNode.close();
        assertEquals(3, threadQty);
    }

    @SneakyThrows
    public void testIndexThreadQty_whenNoValueProvidedByUser_thenDefaultBasedOnCPUCores() {
        // Mock PlatformUtils.getAvailableProcessors() to return a specific value
        try (MockedStatic<PlatformUtils> platformUtilsMock = Mockito.mockStatic(PlatformUtils.class)) {
            // Set up the mock to return 8 processors
            int mockedProcessorCount = 16;
            platformUtilsMock.when(PlatformUtils::getAvailableProcessors).thenReturn(mockedProcessorCount);

            // Create a mock node with empty settings (no user-defined thread qty)
            Node mockNode = createMockNode(Collections.emptyMap());
            mockNode.start();
            ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
            KNNSettings.state().setClusterService(clusterService);

            // Get the actual thread quantity from settings
            int actualThreadQty = KNNSettings.getIndexThreadQty();

            // Expected value should be based on our mocked processor count (8/2 = 4)
            int expectedThreadQty = Math.min(Math.max(1, mockedProcessorCount / 2), 32);

            // Verify that the actual thread quantity matches the expected default
            assertEquals("Thread quantity should be based on mocked CPU cores", expectedThreadQty, actualThreadQty);

            mockNode.close();
        }
    }

    @SneakyThrows
    public void testIndexThreadQty_whenNoValueProvidedByUser_thenDefaultIsWithinValidRange() {
        // Create a mock node with empty settings (no user-defined thread qty)
        Node mockNode = createMockNode(Collections.emptyMap());
        mockNode.start();
        ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
        KNNSettings.state().setClusterService(clusterService);

        // Get the actual thread quantity from settings
        int actualThreadQty = KNNSettings.getIndexThreadQty();

        // The actual value should be within the valid range
        assertTrue("Thread quantity should be at least 1", actualThreadQty >= 1);
        assertTrue("Thread quantity should not exceed 32", actualThreadQty <= 32);

        mockNode.close();
    }

    @SneakyThrows
    public void testIndexThreadQty_whenValueProvidedByUser_thenUserValueIsUsed() {
        int userDefinedThreadQty = 4;

        // Create a mock node with user-defined thread quantity
        Node mockNode = createMockNode(Map.of(KNNSettings.KNN_ALGO_PARAM_INDEX_THREAD_QTY, Integer.toString(userDefinedThreadQty)));
        mockNode.start();
        ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
        KNNSettings.state().setClusterService(clusterService);

        // Get the actual thread quantity from settings
        int actualThreadQty = KNNSettings.getIndexThreadQty();
        mockNode.close();

        // The thread quantity should match the user-defined value
        assertEquals(userDefinedThreadQty, actualThreadQty);
    }

    private Node createMockNode(Map<String, Object> configSettings) throws IOException {
        Path configDir = createTempDir();
        File configFile = configDir.resolve("opensearch.yml").toFile();
        FileWriter configFileWriter = new FileWriter(configFile);

        for (Map.Entry<String, Object> setting : configSettings.entrySet()) {
            configFileWriter.write("\"" + setting.getKey() + "\": " + setting.getValue());
        }
        configFileWriter.close();
        Collection<PluginInfo> plugins = basePlugins().stream()
            .map(
                p -> new PluginInfo(
                    p.getName(),
                    "classpath plugin",
                    "NA",
                    Version.CURRENT,
                    "1.8",
                    p.getName(),
                    null,
                    Collections.emptyList(),
                    false
                )
            )
            .collect(Collectors.toList());
        return new MockNode(baseSettings().build(), plugins, configDir, true);
    }

    private List<Class<? extends Plugin>> basePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(getTestTransportPlugin());
        plugins.add(MockHttpTransport.TestPlugin.class);
        plugins.add(KNNPlugin.class);
        return plugins;
    }

    private static Settings.Builder baseSettings() {
        final Path tempDir = createTempDir();
        return Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), InternalTestCluster.clusterName("single-node-cluster", randomLong()))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
            .put(dataNode());
    }
}
