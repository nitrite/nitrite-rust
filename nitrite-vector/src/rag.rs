//! A thin retrieval-augmented-generation (RAG) store over a Nitrite collection.
//!
//! Each record is a document holding a `text` field, an `embedding` field (the
//! vector), and any caller-supplied metadata. The collection carries a vector
//! index on `embedding`, so [`RagStore::search`] performs kNN and can combine
//! the result with arbitrary Nitrite metadata filters.
//!
//! Embeddings are supplied by the caller; this store does not generate them.
//!
//! The database must be built with a
//! [`VectorModule`](crate::VectorModule) whose
//! [`VectorIndexConfig`](crate::VectorIndexConfig) sets the dimension and the
//! same metric passed here.

use nitrite::collection::{Document, NitriteCollection, NitriteId};
use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::filter::{by_id, Filter};
use nitrite::nitrite::Nitrite;

use crate::distance::Metric;
use crate::filter::{value_to_vector, vector_to_value};
use crate::fluent::vector_field;
use crate::vector_index_options;

/// Document field holding the raw text of a record.
pub const TEXT_FIELD: &str = "text";
/// Document field holding the embedding vector.
pub const EMBEDDING_FIELD: &str = "embedding";

/// How many extra candidates to over-fetch per requested result so that a
/// metadata filter and score cutoff still leave enough hits.
const DEFAULT_OVERSAMPLE: usize = 4;

/// A single search result.
#[derive(Debug, Clone)]
pub struct SearchHit {
    /// The document id.
    pub id: NitriteId,
    /// The stored text.
    pub text: String,
    /// Similarity score (higher is more similar; see [`Metric::score`]).
    pub score: f32,
    /// The full stored document (text, embedding, and metadata).
    pub document: Document,
}

/// A vector store tuned for RAG usage.
#[derive(Clone)]
pub struct RagStore {
    collection: NitriteCollection,
    metric: Metric,
}

impl RagStore {
    /// Opens (or creates) a RAG store backed by the named collection, ensuring
    /// a vector index exists on the `embedding` field.
    ///
    /// `metric` **must** match the one configured on the loaded
    /// [`VectorModule`](crate::VectorModule) for this collection's `embedding`
    /// index; it is used to convert distances into similarity scores, and a
    /// mismatch silently produces wrong scores/`min_score` cutoffs (the index
    /// itself still ranks by its own configured metric).
    pub fn create(db: &Nitrite, name: &str, metric: Metric) -> NitriteResult<Self> {
        let collection = db.collection(name)?;
        if !collection.has_index(vec![EMBEDDING_FIELD])? {
            collection.create_index(vec![EMBEDDING_FIELD], &vector_index_options())?;
        }
        Ok(RagStore { collection, metric })
    }

    /// Adds a record and returns its id.
    pub fn add(
        &self,
        text: impl Into<String>,
        embedding: Vec<f32>,
        metadata: Document,
    ) -> NitriteResult<NitriteId> {
        let mut doc = metadata;
        doc.put(TEXT_FIELD, text.into())?;
        doc.put(EMBEDDING_FIELD, vector_to_value(&embedding))?;
        let result = self.collection.insert(doc)?;
        result
            .affected_nitrite_ids()
            .first()
            .cloned()
            .ok_or_else(|| NitriteError::new("Insert returned no id", ErrorKind::InvalidOperation))
    }

    /// Adds many records, returning their ids in order.
    pub fn add_many(
        &self,
        records: Vec<(String, Vec<f32>, Document)>,
    ) -> NitriteResult<Vec<NitriteId>> {
        records
            .into_iter()
            .map(|(text, emb, meta)| self.add(text, emb, meta))
            .collect()
    }

    /// Fetches a record by id.
    pub fn get(&self, id: &NitriteId) -> NitriteResult<Option<Document>> {
        self.collection.get_by_id(id)
    }

    /// Deletes a record by id, returning whether anything was removed.
    pub fn delete(&self, id: &NitriteId) -> NitriteResult<bool> {
        let result = self.collection.remove(by_id(*id), true)?;
        Ok(!result.affected_nitrite_ids().is_empty())
    }

    /// Number of records in the store.
    pub fn len(&self) -> NitriteResult<u64> {
        self.collection.size()
    }

    /// Whether the store has no records.
    pub fn is_empty(&self) -> NitriteResult<bool> {
        Ok(self.len()? == 0)
    }

    /// The underlying collection, for advanced use.
    pub fn collection(&self) -> &NitriteCollection {
        &self.collection
    }

    /// Begins a nearest-neighbor search for the `k` best matches to `query`.
    pub fn search(&self, query: Vec<f32>, k: usize) -> SearchQuery<'_> {
        SearchQuery {
            store: self,
            query,
            k,
            ef: None,
            min_score: None,
            meta_filter: None,
            oversample: DEFAULT_OVERSAMPLE,
        }
    }
}

/// Builder for a RAG search. Finalize with [`SearchQuery::run`].
pub struct SearchQuery<'a> {
    store: &'a RagStore,
    query: Vec<f32>,
    k: usize,
    ef: Option<usize>,
    min_score: Option<f32>,
    meta_filter: Option<Filter>,
    oversample: usize,
}

impl<'a> SearchQuery<'a> {
    /// Restricts results to those also matching a metadata filter.
    pub fn filter(mut self, filter: Filter) -> Self {
        self.meta_filter = Some(filter);
        self
    }

    /// Sets the query-time `ef` (search width; larger = higher recall).
    pub fn ef(mut self, ef: usize) -> Self {
        self.ef = Some(ef);
        self
    }

    /// Drops hits whose similarity score is below `min_score`.
    pub fn min_score(mut self, min_score: f32) -> Self {
        self.min_score = Some(min_score);
        self
    }

    /// Sets how many extra candidates to over-fetch per requested result when a
    /// metadata filter or score cutoff is in play (default 4).
    pub fn oversample(mut self, factor: usize) -> Self {
        self.oversample = factor.max(1);
        self
    }

    /// Executes the search, returning up to `k` hits ordered by descending
    /// score.
    pub fn run(self) -> NitriteResult<Vec<SearchHit>> {
        if self.k == 0 {
            return Ok(Vec::new());
        }
        let metric = self.store.metric;
        // Over-fetch so metadata filtering / score cutoff still leaves k hits.
        let fetch = if self.meta_filter.is_some() || self.min_score.is_some() {
            self.k.saturating_mul(self.oversample)
        } else {
            self.k
        };

        let mut builder = vector_field(EMBEDDING_FIELD).nearest(self.query.clone(), fetch);
        if let Some(ef) = self.ef {
            builder = builder.ef(ef);
        }
        let mut cursor = self.store.collection.find(builder.build())?;

        // Prepare the query once for scoring (normalizes for cosine).
        let prepared_query = metric.prepare(self.query.clone());

        let mut hits: Vec<SearchHit> = Vec::new();
        for entry in cursor.iter_with_id() {
            let (id, document) = entry?;

            if let Some(mf) = &self.meta_filter {
                if !mf.apply(&document)? {
                    continue;
                }
            }

            let embedding = match document.get(EMBEDDING_FIELD).ok().and_then(|v| value_to_vector(&v)) {
                Some(e) if e.len() == prepared_query.len() => e,
                _ => continue,
            };
            let prepared = metric.prepare(embedding);
            let score = metric.score(metric.distance(&prepared_query, &prepared));

            if let Some(min) = self.min_score {
                if score < min {
                    continue;
                }
            }

            let text = match document.get(TEXT_FIELD) {
                Ok(nitrite::common::Value::String(s)) => s,
                _ => String::new(),
            };
            hits.push(SearchHit { id, text, score, document });
        }

        // Rank by score (descending) and trim to k, independent of the order
        // the cursor produced.
        hits.sort_by(|a, b| b.score.total_cmp(&a.score));
        hits.truncate(self.k);
        Ok(hits)
    }
}
