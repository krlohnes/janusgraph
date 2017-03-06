// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.es;


@PreInitializeConfigOptions
public class ElasticSearchIndex extends AbstractElasticSeachIndex {

    private IndexProvider provider;

    public ElasticSearchIndex(Configuration config) {
      if (!config.contains(INTERFACE) || config.get(INTERFACE) != ElasticSearchSetup.HTTP) {
        //This is either the legacy route or the TransportClient route
        provider = new ElasticSearchIndexDeprecated(config);
      } else {
        provider = new ElasticSearchIndexHttp(config);
      }
    }

    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException {
      provider.register(store, key, information, tx);
    }

    @Override
    public void mutate(Map<String,Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
      provider.mutate(mutations, informations, tx);
    }

    @Override
    public void restore(Map<String,Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
      provider.restore(documents, informations, tx);
    }

    @Override
    public List<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
      provider.query(query, informations, tx);
    }

    @Override
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
      provider.query(query, informations, tx);
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
      provider.beginTransaction(config);
    }

    @Override
    public void close() throws BackendException {
      provider.close();
    }

    @Override
    public void clearStorage() throws BackendException {
      provider.clearStorage();
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
      provider.supports(information, janusgraphPredicate);
    }

    @Override
    public boolean supports(KeyInformation information) {
      provider.supports(information);
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
      provider.mapKey2Field(key, information);
    }

    @Override
    public IndexFeatures getFeatures() {
      provider.getFeatures();
    }
}


