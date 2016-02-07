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
import com.gigaspaces.query.extension.QueryExtensionEntryIterator;
import com.gigaspaces.query.extension.QueryExtensionManager;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;

import java.io.IOException;

/**
 * @author yechielf
 * @since 11.0
 */
public class LuceneSpatialQueryIterator extends QueryExtensionEntryIterator {
    private final ScoreDoc[] scores;
    private final IndexSearcher indexSearcher;
    private final DirectoryReader directoryReader;
    private int _pos;

    public LuceneSpatialQueryIterator(QueryExtensionManager indexManager, String alreadyMatchedIndexPath,
                                      ScoreDoc[] scores, IndexSearcher is, DirectoryReader directoryReader) {
        super(indexManager, alreadyMatchedIndexPath);
        this.scores = scores;
        this.indexSearcher = is;
        this.directoryReader = directoryReader;
    }

    @Override
    public void close() throws IOException {
        directoryReader.close();
        super.close();
    }

    @Override
    public boolean hasNext() {
        return _pos <  scores.length;
    }

    public String nextUid() {
        try {
            Document d = indexSearcher.doc(scores[_pos++].doc);
            return d.get(LuceneSpatialQueryExtensionManager.XAP_ID);
        } catch (IOException e) {
            throw new SpaceRuntimeException("Failed to get next item", e);
        }
    }
}
