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

import com.gigaspaces.SpaceRuntimeException;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.index.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.openspaces.core.util.FileUtils;
import org.openspaces.spatial.shapes.Shape;
import org.openspaces.spatial.spatial4j.Spatial4jShapeProvider;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author yechielf
 * @since 11.0
 */
public class LuceneSpatialQueryExtensionIndexManager extends QueryExtensionIndexManager {
    private static final Logger _logger = Logger.getLogger(LuceneSpatialQueryExtensionIndexManager.class.getName());

    protected static final String XAP_ID = "XAP_ID";
    private static final String XAP_ID_VERSION = "XAP_ID_VERSION";
    private static final int MAX_RESULTS = Integer.MAX_VALUE;

    private final Map<String, LuceneSpatialTypeIndex> _luceneHolderMap = new ConcurrentHashMap<String, LuceneSpatialTypeIndex>();
    private final String _namespace;
    private final LuceneConfiguration _luceneConfiguration;

    public LuceneSpatialQueryExtensionIndexManager(QueryExtensionIndexManagerConfig config) {
        super(config);
        _namespace = config.getNamespace();
        _luceneConfiguration = new LuceneConfiguration(config);
        File location = new File(_luceneConfiguration.getLocation());
        if (location.exists())
            FileUtils.deleteFileOrDirectory(location);
    }

    @Override
    public void close() throws IOException {
        for (LuceneSpatialTypeIndex luceneHolder : _luceneHolderMap.values())
            luceneHolder.close();

        _luceneHolderMap.clear();
        FileUtils.deleteFileOrDirectory(new File(_luceneConfiguration.getLocation()));
        super.close();
    }

    @Override
    public void introduceType(SpaceTypeDescriptor typeDescriptor) {
        super.introduceType(typeDescriptor);
        final String typeName = typeDescriptor.getTypeName();
        if (!_luceneHolderMap.containsKey(typeName)) {
            try {
                _luceneHolderMap.put(typeName, new LuceneSpatialTypeIndex(_luceneConfiguration, _namespace, typeDescriptor));
            } catch (IOException e) {
                throw new SpaceRuntimeException("Failed to introduce type " + typeName, e);
            }
        } else {
            _logger.log(Level.WARNING, "Type [" + typeName + "] is already introduced to geospatial handler");
        }
    }

    @Override
    public boolean insertEntry(IndexableServerEntry entry, boolean removePrevious) {
        final String typeName = entry.getSpaceTypeDescriptor().getTypeName();
        final LuceneSpatialTypeIndex luceneHolder = getLuceneHolder(typeName);
        try {
            final Document doc = createDocumentIfNeeded(luceneHolder, entry);
            if (doc != null || removePrevious) {
                //Add new
                if (doc != null)
                    luceneHolder.getIndexWriter().addDocument(doc);
                //Delete old
                if (removePrevious)
                    luceneHolder.getIndexWriter().deleteDocuments(new TermQuery(new Term(XAP_ID_VERSION,
                            concat(entry.getUid(), entry.getVersion() - 1))));
                luceneHolder.commit(false);
            }
            return doc != null;
        } catch (Exception e) {
            String operation = removePrevious ? "update" : "insert";
            throw new SpaceRuntimeException("Failed to " + operation + " entry of type " + typeName + " with id [" + entry.getUid() + "]", e);
        }
    }

    @Override
    public void removeEntry(SpaceTypeDescriptor typeDescriptor, String uid, int version)
    {
        final String typeName = typeDescriptor.getTypeName();
        final LuceneSpatialTypeIndex luceneHolder = getLuceneHolder(typeName);
        try {
            luceneHolder.getIndexWriter().deleteDocuments(new TermQuery(new Term(XAP_ID_VERSION, concat(uid, version))));
            luceneHolder.commit(false);
        } catch (IOException e) {
            throw new SpaceRuntimeException("Failed to remove entry of type " + typeName, e);
        }
    }

    private LuceneSpatialTypeIndex getLuceneHolder(String className) {
        return _luceneHolderMap.get(className);
    }

    private Document createDocumentIfNeeded(LuceneSpatialTypeIndex luceneHolder, IndexableServerEntry entry) {

        Document doc = null;
        for (String path : luceneHolder.getQueryExtensionInfo().getPaths()) {
            final Object fieldValue = entry.getPathValue(path);
            if (fieldValue instanceof Shape) {
                final SpatialStrategy strategy = createStrategyByFieldName(path);
                final Field[] fields = strategy.createIndexableFields(toSpatial4j((Shape) fieldValue));
                if (doc == null)
                    doc = new Document();
                for (Field field : fields)
                    doc.add(field);
            }
        }
        if (doc != null) {
            //cater for uid & version
            //noinspection deprecation
            doc.add(new Field(XAP_ID, entry.getUid(), Field.Store.YES, Field.Index.NO));
            //noinspection deprecation
            doc.add(new Field(XAP_ID_VERSION, concat(entry.getUid(), entry.getVersion()), Field.Store.YES, Field.Index.NOT_ANALYZED));
        }

        return doc;
    }

    public com.spatial4j.core.shape.Shape toSpatial4j(Shape gigaShape) {
        if (gigaShape instanceof Spatial4jShapeProvider)
            return ((Spatial4jShapeProvider)gigaShape).getSpatial4jShape(_luceneConfiguration.getSpatialContext());
        throw new IllegalArgumentException("Unsupported shape [" + gigaShape.getClass().getName() + "]");
    }

    private SpatialStrategy createStrategyByFieldName(String fieldName) {
        return _luceneConfiguration.getStrategy(fieldName);
    }

    public boolean filter(String operation, Object leftOperand, Object rightOperand) {
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "filter [operation=" + operation + ", leftOperand=" + leftOperand + ", rightOperand=" + rightOperand + "]");

        if (!(leftOperand instanceof Shape) || !(rightOperand instanceof Shape))
            throw new IllegalArgumentException("Operation " + operation + " can be applied only for geometrical shapes, instead given: " + leftOperand + " and " + rightOperand);
        SpatialOp spatialOperation;
        try {
            spatialOperation = SpatialOp.valueOf(operation.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Operation " + operation + " not found - supported operations: " + Arrays.asList(SpatialOp.values()));
        }
        com.spatial4j.core.shape.Shape shape1 = toSpatial4j((Shape) leftOperand);
        com.spatial4j.core.shape.Shape shape2 = toSpatial4j((Shape) rightOperand);
        return spatialOperation.evaluate(shape1, shape2);
    }

    @Override
    public QueryExtensionIndexEntryIterator scanIndex(String typeName, String path, String operation, Object operand) {
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "scanIndex [typeName=" + typeName + ", path=" + path + ", operation=" + operation + ", operand=" + operand + "]");

        final Query query = createQuery(path, operand, operation);
        final LuceneSpatialTypeIndex luceneHolder = getLuceneHolder(typeName);
        try {
            // Flush
            luceneHolder.commit(true);

            DirectoryReader dr = DirectoryReader.open(luceneHolder.getDirectory());
            IndexSearcher is = new IndexSearcher(dr);
            ScoreDoc[] scores = is.search(query, MAX_RESULTS).scoreDocs;
            String alreadyMatchedIndexPath = _luceneConfiguration.rematchAlreadyMatchedIndexPath(path) ? null : path;
            return new LuceneSpatialQueryIndexIterator(this, alreadyMatchedIndexPath, scores, is, dr);
        } catch (IOException e) {
            throw new SpaceRuntimeException("Failed to scan index", e);
        }
    }

    private enum SpatialOp {
        WITHIN(SpatialOperation.IsWithin),
        CONTAINS(SpatialOperation.Contains),
        INTERSECTS(SpatialOperation.Intersects);
        /*,
        DISJOINT(SpatialOperation.IsDisjointTo) {
            @Override
            public Query makeQuery(SpatialStrategy spatialStrategy, com.spatial4j.core.shape.Shape subjectShape) {
                SpatialArgs intersectsArgs = new SpatialArgs(SpatialOperation.Intersects, subjectShape);
                Query intersectsQuery = spatialStrategy.makeQuery(intersectsArgs);

                return new BooleanQuery.Builder()
                        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
                        .add(intersectsQuery, BooleanClause.Occur.MUST_NOT)
                        .build();
            }
        };*/

        private final SpatialOperation _spatialOperation;

        SpatialOp(SpatialOperation spatialOperation) {
            this._spatialOperation = spatialOperation;
        }

        public Query makeQuery(SpatialStrategy spatialStrategy, com.spatial4j.core.shape.Shape subjectShape) {
            SpatialArgs args = new SpatialArgs(_spatialOperation, subjectShape);
            return spatialStrategy.makeQuery(args);
        }

        public boolean evaluate(com.spatial4j.core.shape.Shape indexedShape, com.spatial4j.core.shape.Shape queryShape) {
            return _spatialOperation.evaluate(indexedShape, queryShape);
        }
    }

    private Query createQuery(String path, Object operand, String operation) {
        final SpatialOp spatialOp = SpatialOp.valueOf(operation.toUpperCase());
        if (!(operand instanceof Shape))
            throw new IllegalArgumentException("Operation " + operation + " can be applied only for geometrical shapes, instead given: " + operand);
        com.spatial4j.core.shape.Shape subjectShape = toSpatial4j((Shape) operand);
        return spatialOp.makeQuery(createStrategyByFieldName(path), subjectShape);
    }

    protected String concat(String uid, int version) {
        return uid + "_" + version;
    }
}
