/*******************************************************************************
 *
 * Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.openspaces.spatial.internal;

import com.gigaspaces.query.extension.index.QueryExtensionIndexManagerConfig;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.SpatialContextFactory;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContextFactory;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.bbox.BBoxStrategy;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * @author Yohana Khoury
 * @since 11.0
 */
public class LuceneConfiguration {
    public static final String FILE_SEPARATOR = File.separator;
    public static final String SPATIAL_PREFIX = "spatial";

    //space-config.spatial.lucene.strategy
    public static final String STRATEGY = SPATIAL_PREFIX + ".lucene.strategy";
    public static final String STRATEGY_DEFAULT = SupportedSpatialStrategy.RecursivePrefixTree.name();

    //space-config.spatial.lucene.strategy.spatial-prefix-tree
    public static final String SPATIAL_PREFIX_TREE = SPATIAL_PREFIX + ".lucene.strategy.spatial-prefix-tree";
    public static final String SPATIAL_PREFIX_TREE_DEFAULT = SupportedSpatialPrefixTree.GeohashPrefixTree.name();
    //space-config.spatial.lucene.strategy.spatial-prefix-tree.max-levels
    public static final String SPATIAL_PREFIX_TREE_MAX_LEVELS = SPATIAL_PREFIX + ".lucene.strategy.spatial-prefix-tree.max-levels";
    public static final String SPATIAL_PREFIX_TREE_MAX_LEVELS_DEFAULT = "11";
    //space-config.spatial.lucene.strategy.dist-err-pct
    public static final String DIST_ERR_PCT = SPATIAL_PREFIX + ".lucene.strategy.distance-error-pct";
    public static final String DIST_ERR_PCT_DEFAULT = "0.025";

    //space-config.spatial.lucene.storage.directory-type
    public static final String STORAGE_DIRECTORYTYPE = SPATIAL_PREFIX + ".lucene.storage.directory-type";
    public static final String STORAGE_DIRECTORYTYPE_DEFAULT = SupportedDirectory.MMapDirectory.name();
    //space-config.spatial.lucene.storage.location
    public static final String STORAGE_LOCATION = SPATIAL_PREFIX + ".lucene.storage.location";

    //space-config.spatial.context
    public static final String SPATIAL_CONTEXT = SPATIAL_PREFIX + ".context";
    public static final String SPATIAL_CONTEXT_DEFAULT = SupportedSpatialContext.JTS.name();

    //space-config.spatial.context.geo
    public static final String SPATIAL_CONTEXT_GEO = SPATIAL_PREFIX + ".context.geo";
    public static final String SPATIAL_CONTEXT_GEO_DEFAULT = "true";

    //space-config.spatial.context.world-bounds, default is set by lucene
    public static final String SPATIAL_CONTEXT_WORLD_BOUNDS = SPATIAL_PREFIX + ".context.world-bounds";

    private final SpatialContext _spatialContext;
    private final StrategyFactory _strategyFactory;
    private final DirectoryFactory _directoryFactory;
    private final int _maxUncommittedChanges;
    private String _location;

    private enum SupportedSpatialStrategy {
        RecursivePrefixTree, BBox, Composite;
        public static SupportedSpatialStrategy byName (String key) {
            for (SupportedSpatialStrategy spatialStrategy : SupportedSpatialStrategy.values())
                if (spatialStrategy.name().equalsIgnoreCase(key))
                    return spatialStrategy;

            throw new IllegalArgumentException("Unsupported Spatial strategy: " + key + " - supported values: " + Arrays.asList(values()));
        }
    }

    private enum SupportedSpatialPrefixTree {
        GeohashPrefixTree, QuadPrefixTree;
        public static SupportedSpatialPrefixTree byName (String key) {
            for (SupportedSpatialPrefixTree spatialPrefixTree : SupportedSpatialPrefixTree.values())
                if (spatialPrefixTree.name().equalsIgnoreCase(key))
                    return spatialPrefixTree;


            throw new IllegalArgumentException("Unsupported spatial prefix tree: " + key + " - supported values: " + Arrays.asList(values()));
        }
    }

    private enum SupportedSpatialContext {
        Spatial4J, JTS;

        public static SupportedSpatialContext byName (String key) {
            for (SupportedSpatialContext spatialContext : SupportedSpatialContext.values())
                if (spatialContext.name().equalsIgnoreCase(key))
                    return spatialContext;

            throw new IllegalArgumentException("Unsupported spatial context: " + key + " - supported values: " + Arrays.asList(values()));
        }
    }

    private enum SupportedDirectory {
        MMapDirectory, RAMDirectory;
        public static SupportedDirectory byName (String key) {
            for (SupportedDirectory directory : SupportedDirectory.values())
                if (directory.name().equalsIgnoreCase(key))
                    return directory;

            throw new IllegalArgumentException("Unsupported directory: " + key + " - supported values: " + Arrays.asList(values()));
        }
    }

    public LuceneConfiguration(QueryExtensionIndexManagerConfig config) {
        this._spatialContext = createSpatialContext(config);
        this._strategyFactory = createStrategyFactory(config);
        this._directoryFactory = createDirectoryFactory(config);
        //TODO: read from config
        this._maxUncommittedChanges = 1000;
    }

    private static RectangleImpl createSpatialContextWorldBounds(QueryExtensionIndexManagerConfig config)  {
        String spatialContextWorldBounds = config.getSpaceProperty(SPATIAL_CONTEXT_WORLD_BOUNDS, null);
        if (spatialContextWorldBounds == null)
            return null;

        String[] tokens = spatialContextWorldBounds.split(",");
        if (tokens.length != 4)
            throw new IllegalArgumentException("World bounds ["+spatialContextWorldBounds+"] must be of format: minX, maxX, minY, maxY");
        double[] worldBounds = new double[tokens.length];
        for (int i=0 ; i < worldBounds.length ; i++) {
            try {
                worldBounds[i] = Double.parseDouble(tokens[i].trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid world bounds [" + spatialContextWorldBounds + "] - token #" + (i+1) + " is not a number");
            }
        }

        double minX = worldBounds[0];
        double maxX = worldBounds[1];
        double minY = worldBounds[2];
        double maxY = worldBounds[3];
        if (!((minX <= maxX) && (minY <= maxY)))
            throw new IllegalStateException("Values of world bounds [minX, maxX, minY, maxY]=["+spatialContextWorldBounds+"] must meet: minX<=maxX, minY<=maxY");

        return new RectangleImpl(minX, maxX, minY, maxY, null);
    }

    private static SpatialContext createSpatialContext(QueryExtensionIndexManagerConfig config) {
        String spatialContextString = config.getSpaceProperty(SPATIAL_CONTEXT, SPATIAL_CONTEXT_DEFAULT);
        SupportedSpatialContext spatialContext = SupportedSpatialContext.byName(spatialContextString);
        boolean geo = Boolean.valueOf(config.getSpaceProperty(SPATIAL_CONTEXT_GEO, SPATIAL_CONTEXT_GEO_DEFAULT));
        RectangleImpl worldBounds = createSpatialContextWorldBounds(config);

        switch (spatialContext) {
            case JTS: {
                JtsSpatialContextFactory factory = new JtsSpatialContextFactory();
                factory.geo = geo;
                if (worldBounds != null)
                    factory.worldBounds = worldBounds;
                return new JtsSpatialContext(factory);
            }
            case Spatial4J: {
                SpatialContextFactory factory = new SpatialContextFactory();
                factory.geo = geo;
                if (worldBounds != null)
                    factory.worldBounds = worldBounds;
                return new SpatialContext(factory);
            }
            default:
                throw new IllegalStateException("Unsupported spatial context type " + spatialContext);
        }
    }

    protected StrategyFactory createStrategyFactory(QueryExtensionIndexManagerConfig config) {
        String strategyString = config.getSpaceProperty(STRATEGY, STRATEGY_DEFAULT);
        SupportedSpatialStrategy spatialStrategy = SupportedSpatialStrategy.byName(strategyString);

        switch (spatialStrategy) {
            case RecursivePrefixTree: {
                final SpatialPrefixTree geohashPrefixTree = createSpatialPrefixTree(config, _spatialContext);
                String distErrPctValue = config.getSpaceProperty(DIST_ERR_PCT, DIST_ERR_PCT_DEFAULT);
                final double distErrPct = Double.valueOf(distErrPctValue);

                return new StrategyFactory(spatialStrategy) {
                    @Override
                    public SpatialStrategy createStrategy(String fieldName) {
                        RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(geohashPrefixTree, fieldName);
                        strategy.setDistErrPct(distErrPct);
                        return strategy;
                    }
                };
            }
            case BBox: {
                return new StrategyFactory(spatialStrategy) {
                    @Override
                    public SpatialStrategy createStrategy(String fieldName) {
                        return new BBoxStrategy(_spatialContext, fieldName);
                    }
                };
            }
            case Composite: {
                final SpatialPrefixTree geohashPrefixTree = createSpatialPrefixTree(config, _spatialContext);
                String distErrPctValue = config.getSpaceProperty(DIST_ERR_PCT, DIST_ERR_PCT_DEFAULT);
                final double distErrPct = Double.valueOf(distErrPctValue);

                return new StrategyFactory(spatialStrategy) {
                    @Override
                    public SpatialStrategy createStrategy(String fieldName) {
                        RecursivePrefixTreeStrategy recursivePrefixTreeStrategy = new RecursivePrefixTreeStrategy(geohashPrefixTree, fieldName);
                        recursivePrefixTreeStrategy.setDistErrPct(distErrPct);
                        SerializedDVStrategy serializedDVStrategy = new SerializedDVStrategy(_spatialContext, fieldName);
                        return new CompositeSpatialStrategy(fieldName, recursivePrefixTreeStrategy, serializedDVStrategy);
                    }
                };
            }
            default:
                throw new IllegalStateException("Unsupported strategy: " + spatialStrategy);
        }
    }

    private static SpatialPrefixTree createSpatialPrefixTree(QueryExtensionIndexManagerConfig config, SpatialContext spatialContext) {
        String spatialPrefixTreeType = config.getSpaceProperty(SPATIAL_PREFIX_TREE, SPATIAL_PREFIX_TREE_DEFAULT);

        SupportedSpatialPrefixTree spatialPrefixTree = SupportedSpatialPrefixTree.byName(spatialPrefixTreeType);
        String maxLevelsStr = config.getSpaceProperty(SPATIAL_PREFIX_TREE_MAX_LEVELS, SPATIAL_PREFIX_TREE_MAX_LEVELS_DEFAULT);
        int maxLevels = Integer.valueOf(maxLevelsStr);

        switch (spatialPrefixTree) {
            case GeohashPrefixTree:
                return new GeohashPrefixTree(spatialContext, maxLevels);
            case QuadPrefixTree:
                return new QuadPrefixTree(spatialContext, maxLevels);
            default:
                throw new RuntimeException("Unhandled spatial prefix tree type: " + spatialPrefixTree);
        }
    }

    protected DirectoryFactory createDirectoryFactory(QueryExtensionIndexManagerConfig config) {
        String directoryType = config.getSpaceProperty(STORAGE_DIRECTORYTYPE, STORAGE_DIRECTORYTYPE_DEFAULT);
        SupportedDirectory directory = SupportedDirectory.byName(directoryType);

        switch (directory) {
            case MMapDirectory: {
                //try space-config.spatial.lucene.storage.location first, if not configured then use workingDir.
                //If workingDir == null (Embedded space , Integrated PU , etc...) then use process working dir (user.dir)
                _location = config.getSpaceProperty(STORAGE_LOCATION, (config.getWorkDir() == null ? System.getProperty("user.dir")+FILE_SEPARATOR+"luceneIndex" : config.getWorkDir() + FILE_SEPARATOR + "luceneIndex"));

                return new DirectoryFactory() {
                    @Override
                    public Directory getDirectory(String relativePath) throws IOException {
                        return new MMapDirectory(Paths.get(_location+FILE_SEPARATOR+relativePath));
                    }
                };
            }
            case RAMDirectory: {
                return new DirectoryFactory() {
                    @Override
                    public Directory getDirectory(String path) throws IOException {
                        return new RAMDirectory();
                    }
                };
            }
            default:
                throw new RuntimeException("Unhandled directory type " + directory);
        }
    }


    public SpatialStrategy getStrategy(String fieldName) {
        return this._strategyFactory.createStrategy(fieldName);
    }

    public Directory getDirectory(String relativePath) throws IOException {
        return _directoryFactory.getDirectory(relativePath);
    }

    public SpatialContext getSpatialContext() {
        return _spatialContext;
    }

    public int getMaxUncommittedChanges() {
        return _maxUncommittedChanges;
    }

    public String getLocation() {
        return _location;
    }

    public boolean rematchAlreadyMatchedIndexPath(String path) {
        //always return true for now, should be: return _strategyFactory.getStrategyName().equals(SupportedSpatialStrategy.CompositeSpatialStrategy);
        return true;
    }

    public abstract class StrategyFactory {
        private SupportedSpatialStrategy _strategyName;

        public StrategyFactory(SupportedSpatialStrategy strategyName) {
            this._strategyName = strategyName;
        }

        public abstract SpatialStrategy createStrategy(String fieldName);

        public SupportedSpatialStrategy getStrategyName() {
            return _strategyName;
        }
    }

    public abstract class DirectoryFactory {
        public abstract Directory getDirectory(String relativePath) throws IOException;
    }

}
