package org.openspaces.spatial;

import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.bbox.BBoxStrategy;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.openspaces.spatial.spi.LuceneSpatialConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * @author Yohana Khoury
 * @since 11.0
 */
public class LuceneConfigurationTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @After
    public void tearDown() throws IOException {
        File luceneIndexFolder = new File(System.getProperty("user.dir") + "/luceneIndex");
        if (luceneIndexFolder.exists()) {
            deleteFileOrDirectory(luceneIndexFolder);
        }
    }

    public static void deleteFileOrDirectory(File fileOrDirectory) {
        if (fileOrDirectory.isDirectory()) {
            for (File file : fileOrDirectory.listFiles())
                deleteFileOrDirectory(file);
        }
        if (!fileOrDirectory.delete()) {
            throw new RuntimeException("Failed to delete " + fileOrDirectory);
        }
    }

    private String getWorkingDir() {
        return temporaryFolder.getRoot().getAbsolutePath();
    }

    @Test
    public void testDefaults() throws IOException {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());
        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(config);

        //test directory
        Directory directory = luceneConfiguration.getDirectory("A");
        Assert.assertEquals("MMapDirectory should be the default directory", MMapDirectory.class, directory.getClass());
        String expectedLocation = getWorkingDir()+ File.separator + "spatial" + File.separator + config.getSpaceInstanceName();
        Assert.assertEquals("Default location", expectedLocation, luceneConfiguration.getLocation());
        Assert.assertEquals("MMapDirectory location should be workingDir/luceneIndex/A "+expectedLocation+"/A", new MMapDirectory(Paths.get(expectedLocation + "/A")).getDirectory(), ((MMapDirectory) directory).getDirectory());

        //test strategy
        SpatialStrategy strategy = luceneConfiguration.getStrategy("myfield");
        Assert.assertEquals("Default strategy should be RecursivePrefixTree", RecursivePrefixTreeStrategy.class, strategy.getClass());
        Assert.assertEquals("Unexpected spatial context", JtsSpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertTrue("DistErrPct default for strategy should be 0.025", 0.025 == ((RecursivePrefixTreeStrategy) strategy).getDistErrPct());

        //test spatialprefixtree
        Assert.assertEquals("GeohashPrefixTree should be the default spatial prefix tree for strategy", GeohashPrefixTree.class, ((RecursivePrefixTreeStrategy) strategy).getGrid().getClass());
        Assert.assertEquals("MaxLevels should be 11 as default", 11, ((RecursivePrefixTreeStrategy) strategy).getGrid().getMaxLevels());

        //test spatialcontext
        Assert.assertEquals("Default spatialcontext should be JTS", JtsSpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Default spatialcontext.geo should be true", true, luceneConfiguration.getSpatialContext().isGeo());

    }

    @Test
    public void testInvalidDirectoryType() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.storage.directory-type", "A");
        try {
            new LuceneSpatialConfiguration(config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("Unsupported directory: A - supported values: [MMapDirectory, RAMDirectory]", e.getMessage());
        }
    }

    @Test
    public void testRAMDirectoryTypeCaseInsensitive() throws IOException {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.storage.directory-type", "Ramdirectory")
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(config);

        Directory directory = luceneConfiguration.getDirectory("unused");
        Assert.assertEquals("Unexpected Directory type", RAMDirectory.class, directory.getClass());
    }

    @Test
    public void testMMapDirectoryTypeAndLocation() throws IOException {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.storage.directory-type", "MMapDirectory")
                .setSpaceProperty("spatial.lucene.storage.location", temporaryFolder.getRoot().getAbsolutePath() + "/tempdir")
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(config);
        Directory directory = luceneConfiguration.getDirectory("subfolder");

        Assert.assertEquals("Unexpected Directory type", MMapDirectory.class, directory.getClass());
        Assert.assertEquals(temporaryFolder.getRoot().getAbsolutePath()
                        + File.separator + "tempdir"
                        + File.separator + config.getSpaceInstanceName()
                        + File.separator + "subfolder",
                ((MMapDirectory) directory).getDirectory().toFile().getAbsolutePath());
    }

    @Test
    public void testLocationNoWorkingDir() throws IOException {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.storage.directory-type", "MMapDirectory")
                .setWorkDir(null); //null as second parameter simulates there is no working dir (not pu)

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(config);
        Directory directory = luceneConfiguration.getDirectory("subfolder");

        Assert.assertEquals("Unexpected Directory type", MMapDirectory.class, directory.getClass());
        Assert.assertEquals(System.getProperty("user.dir") +
                        File.separator + "xap" +
                        File.separator + "spatial" +
                        File.separator + config.getSpaceInstanceName() +
                        File.separator + "subfolder",
                ((MMapDirectory) directory).getDirectory().toFile().getAbsolutePath());
    }

    @Test
    public void testInvalidSpatialContextTree() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.strategy.spatial-prefix-tree", "invalidValue")
                .setWorkDir(getWorkingDir());
        try {
            new LuceneSpatialConfiguration(config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("Unsupported spatial prefix tree: invalidValue - supported values: [GeohashPrefixTree, QuadPrefixTree]", e.getMessage());
        }
    }

    @Test
    public void testSpatialContextTreeQuadPrefixTreeAndMaxLevels() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.strategy.spatial-prefix-tree", "QuadPrefixTree")
                .setSpaceProperty("spatial.lucene.strategy.spatial-prefix-tree.max-levels", "20")
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(config);

        SpatialStrategy strategy = luceneConfiguration.getStrategy("myField");
        Assert.assertEquals("Unexpected spatial prefix tree", QuadPrefixTree.class, ((RecursivePrefixTreeStrategy) strategy).getGrid().getClass());
        Assert.assertEquals("MaxLevels should be 20", 20, ((RecursivePrefixTreeStrategy) strategy).getGrid().getMaxLevels());
    }

    @Test
    public void testInvalidSpatialContext() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.context", "dummy")
                .setWorkDir(getWorkingDir());

        try {
            new LuceneSpatialConfiguration(config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("Unsupported spatial context: dummy - supported values: [Spatial4J, JTS]", e.getMessage());
        }
    }

    @Test
    public void testSpatialContextGEO() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.context", "spatial4j")
                .setSpaceProperty("spatial.context.geo", "true")
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(config);

        Assert.assertEquals("Unexpected spatial context", SpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Expecting geo spatial context", true, luceneConfiguration.getSpatialContext().isGeo());
    }

    @Test
    public void testSpatialContextNonGEO() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.strategy.spatial-prefix-tree", "QuadPrefixTree")
                .setSpaceProperty("spatial.context", "spatial4j")
                .setSpaceProperty("spatial.context.geo", "false")
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(config);

        Assert.assertEquals("Unexpected spatial context", SpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Expecting geo spatial context", false, luceneConfiguration.getSpatialContext().isGeo());
    }

    @Test
    public void testSpatialContextJTSNonGEO() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.strategy.spatial-prefix-tree", "QuadPrefixTree")
                .setSpaceProperty("spatial.context", "jts")
                .setSpaceProperty("spatial.context.geo", "false")
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(config);

        Assert.assertEquals("Unexpected spatial context", JtsSpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Expecting geo spatial context", false, luceneConfiguration.getSpatialContext().isGeo());
    }

    @Test
    public void testSpatialContextGEOInvalidWorldBoundsPropertyValue() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.context", "jts")
                .setSpaceProperty("spatial.context.geo", "true")
                .setSpaceProperty("spatial.context.world-bounds", "invalidvaluehere");

        try {
            new LuceneSpatialConfiguration(config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("World bounds [invalidvaluehere] must be of format: minX, maxX, minY, maxY", e.getMessage());
        }
    }

    @Test
    public void testSpatialContextNONGEOInvalidWorldBoundsValues() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.context", "spatial4J")
                .setSpaceProperty("spatial.context.geo", "false")
                .setSpaceProperty("spatial.context.world-bounds", "1,7,9,1");

        try {
            new LuceneSpatialConfiguration(config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("Values of world bounds [minX, maxX, minY, maxY]=[1,7,9,1] must meet: minX<=maxX, minY<=maxY", e.getMessage());
        }
    }

    @Test
    public void testSpatialContextJTSGEOInvalidWorldBoundsStringValue() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.context", "jts")
                .setSpaceProperty("spatial.context.geo", "true")
                .setSpaceProperty("spatial.context.world-bounds", "1,7,1,4a");

        try {
            new LuceneSpatialConfiguration(config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("Invalid world bounds [1,7,1,4a] - token #4 is not a number", e.getMessage());
        }
    }

    @Test
    public void testSpatialContextJTSNONGEO() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.strategy.spatial-prefix-tree", "QuadPrefixTree")
                .setSpaceProperty("spatial.context", "jts")
                .setSpaceProperty("spatial.context.geo", "false")
                .setSpaceProperty("spatial.context.world-bounds", "1,10,-100,100")
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(config);

        Assert.assertEquals("Unexpected spatial context", JtsSpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Expecting geo spatial context", false, luceneConfiguration.getSpatialContext().isGeo());
        Assert.assertEquals("Unexpected spatial context world bound", new RectangleImpl(1, 10, -100, 100, null), luceneConfiguration.getSpatialContext().getWorldBounds());
    }

    @Test
    public void testInvalidStrategy() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.strategy", "mystrategy");

        try {
            new LuceneSpatialConfiguration(config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("Unsupported Spatial strategy: mystrategy - supported values: [RecursivePrefixTree, BBox, Composite]", e.getMessage());
        }
    }

    @Test
    public void testStrategyRecursivePrefixTreeAndDistErrPct() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.strategy", "RecursivePrefixTree")
                .setSpaceProperty("spatial.lucene.strategy.spatial-prefix-tree", "GeohashPrefixTree")
                .setSpaceProperty("spatial.lucene.strategy.spatial-prefix-tree.max-levels", "10")
                .setSpaceProperty("spatial.lucene.strategy.distance-error-pct", "0.5")
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(config);

        Assert.assertEquals("Unexpected strategy type", RecursivePrefixTreeStrategy.class, luceneConfiguration.getStrategy("myField").getClass());
        RecursivePrefixTreeStrategy strategy = (RecursivePrefixTreeStrategy) luceneConfiguration.getStrategy("myField");
        Assert.assertEquals("Unexpected spatial prefix tree", GeohashPrefixTree.class, strategy.getGrid().getClass());
        Assert.assertEquals("MaxLevels should be 10", 10, strategy.getGrid().getMaxLevels());
        Assert.assertTrue("Expecting distance-error-pct to be 0.5", 0.5 == strategy.getDistErrPct());
    }

    @Test
    public void testStrategyBBox() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.strategy", "BBox")
                .setSpaceProperty("spatial.context", "spatial4J")
                .setSpaceProperty("spatial.context.geo", "false")
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(config);

        Assert.assertEquals("Unexpected strategy type", BBoxStrategy.class, luceneConfiguration.getStrategy("myField").getClass());
        Assert.assertEquals("Unexpected spatial context", SpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Expecting geo spatial context", false, luceneConfiguration.getSpatialContext().isGeo());
    }

    @Test
    public void testStrategyComposite() {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setSpaceProperty("spatial.lucene.strategy", "composite")
                .setSpaceProperty("spatial.context", "spatial4J")
                .setSpaceProperty("spatial.context.geo", "true")
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(config);

        Assert.assertEquals("Unexpected strategy type", CompositeSpatialStrategy.class, luceneConfiguration.getStrategy("myField").getClass());
        Assert.assertEquals("Unexpected spatial context", SpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Expecting geo spatial context", true, luceneConfiguration.getSpatialContext().isGeo());
    }

    private static class MockConfig implements QueryExtensionRuntimeInfo {
        private Properties properties = new Properties();
        private String workDir;

        @Override
        public String getSpaceInstanceName() {
            return "foo";
        }

        @Override
        public String getSpaceInstanceWorkDirectory() {
            return workDir;
        }

        public MockConfig setWorkDir(String workDir) {
            this.workDir = workDir;
            return this;
        }

        @Override
        public String getSpaceProperty(String key, String defaultValue) {
            return properties.getProperty(key, defaultValue);
        }

        public MockConfig setSpaceProperty(String key, String value) {
            properties.setProperty(key, value);
            return this;
        }
    }
}