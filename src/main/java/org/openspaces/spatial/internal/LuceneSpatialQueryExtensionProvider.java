/*******************************************************************************
 *
 * Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
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

import com.gigaspaces.query.extension.index.QueryExtensionManager;
import com.gigaspaces.query.extension.QueryExtensionProvider;
import com.gigaspaces.query.extension.index.QueryExtensionManagerConfig;
import com.gigaspaces.query.extension.metadata.PathQueryExtension;
import org.openspaces.spatial.SpaceSpatialIndex;
import org.openspaces.spatial.SpaceSpatialIndexes;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
public class LuceneSpatialQueryExtensionProvider extends QueryExtensionProvider {

    @Override
    public String getNamespace() {
        return "spatial";
    }

    @Override
    public QueryExtensionManager createManager(QueryExtensionManagerConfig config) {
        return new LuceneSpatialQueryExtensionManager(config);
    }

    @Override
    public Map<String, PathQueryExtension> toPropertyInfo(String property, Annotation annotation) {
        Map<String, PathQueryExtension> result = new HashMap<String, PathQueryExtension>();
        if (annotation instanceof SpaceSpatialIndex) {
            SpaceSpatialIndex index = (SpaceSpatialIndex) annotation;
            result.put(path(property, index), new PathQueryExtension());
        } else if (annotation instanceof SpaceSpatialIndexes) {
            SpaceSpatialIndex[] indexes = ((SpaceSpatialIndexes)annotation).value();
            for (SpaceSpatialIndex index : indexes)
                result.put(path(property, index), new PathQueryExtension());
        }
        return result;
    }

    private static String path(String property, SpaceSpatialIndex index) {
        return index.path().length() == 0 ? property : property + "." + index.path();
    }
}
